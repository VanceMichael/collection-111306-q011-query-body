import {randomUUID} from 'node:crypto';
import {encodeQueryChunks} from './domain.mjs';
import {sendOnce} from './transfer.mjs';

const sleep=(ms)=>new Promise(resolve=>setTimeout(resolve,ms));

// 只读追溯查询的领取流程：
// 1. 查询体先暂存落盘（或领取重启前已暂存的同一份），之后每次尝试都从暂存文件完整重放；
// 2. 307/308 且目标在同一授权范围内时保持 POST 与查询体重发；其余跳转不擅自改成 GET；
// 3. 凭据只发给授权范围内的目的地；
// 4. 是否重发由查询能力（只读、幂等）与已确认的传输事实决定，不对所有错误一律重发；
// 5. 网关回执的方法、摘要、字节数与本地暂存对照；
// 6. 取消时中止传输并清理暂存资源，不留下可被复用的残缺查询。
export async function claimQuery(options){
 const {
  area,id=`claim-${randomUUID()}`,batches,chunks,url,
  credentials={},allowedOrigins,
  capability={readonly:true,idempotent:true},
  maxAttempts=3,maxRedirects=5,backoffMs=25,
  continueTimeoutMs=5000,attemptTimeoutMs=30000,
  signal,
 }=options;
 if(!area)throw new Error('缺少暂存区');
 if(!url)throw new Error('缺少查询地址');
 const audit=[];
 const note=(entry)=>{audit.push({at:new Date().toISOString(),...entry});};
 const approved=new Set(allowedOrigins??[new URL(url).origin]);
 if(!approved.has(new URL(url).origin))throw new Error('初始查询地址不在授权范围内');
 const credentialHeaders=(target)=>approved.has(new URL(target).origin)?credentials:{};
 const cancel=async(during)=>{
  note({phase:'cancelled',during,reason:'操作者取消'});
  await area.release(id);
  note({phase:'cleanup',action:'released',id,reason:'取消后清理暂存资源，不留残缺查询'});
  return {status:'cancelled',id,audit};
 };

 // 领取已有暂存体，或把查询体流式暂存落盘。
 let staged;
 const existing=await area.claim(id).catch(()=>null);
 if(existing){
  staged=existing;
  note({phase:'staging',action:'reused',id,digest:staged.digest,bytes:staged.bytes});
 }else{
  const source=chunks??(batches?encodeQueryChunks(batches):null);
  if(!source)throw new Error('没有可领取的暂存体，也未提供查询内容');
  note({phase:'staging',action:'started',id});
  try{staged=await area.stage(id,source,{signal});}
  catch(error){
   if(error.code==='ERR_CLAIM_CANCELLED')return cancel('staging');
   throw error;
  }
  note({phase:'staging',action:'completed',id,digest:staged.digest,bytes:staged.bytes});
 }

 let current=url;let redirects=0;let attempt=0;let receipt=null;
 while(true){
  if(signal?.aborted)return cancel('transfer');
  attempt+=1;
  const headers={
   'content-type':'application/json; charset=utf-8',
   'x-content-sha256':staged.digest,
   ...credentialHeaders(current),
  };
  const result=await sendOnce({url:current,method:'POST',headers,openBody:()=>area.openBody(id),bytes:staged.bytes,signal,continueTimeoutMs,attemptTimeoutMs});
  note({phase:'attempt',attempt,url:current,method:'POST',digest:staged.digest,bytes:staged.bytes,bytesSent:result.bytesSent,outcome:result.kind,status:result.status,detail:result.detail});
  if(result.kind==='cancelled')return cancel('transfer');

  let decision;
  if(result.kind==='responded'){
   const {status,headers:responseHeaders}=result;
   if(status>=200&&status<300){
    try{receipt=JSON.parse(result.text);}catch{receipt=null;}
    let match=null;
    if(receipt&&typeof receipt==='object'){
     const checks=[];
     if('digest' in receipt)checks.push(receipt.digest===staged.digest);
     if('bytes' in receipt)checks.push(receipt.bytes===staged.bytes);
     if('method' in receipt)checks.push(receipt.method==='POST');
     if(checks.length)match=checks.every(Boolean);
    }
    note({phase:'receipt',attempt,match,server:receipt??undefined});
    if(match===false)decision={action:'stop',terminal:'failed',reason:'网关回执与暂存查询体不一致，停止'};
    else decision={action:'done',reason:'网关已确认接收'};
   }else if(status===307||status===308){
    const location=responseHeaders.location;
    if(!location)decision={action:'stop',terminal:'failed',reason:`跳转响应（${status}）缺少目标地址`};
    else{
     const next=new URL(location,current);
     if(!approved.has(next.origin))decision={action:'stop',terminal:'failed',reason:`跳转目标 ${next.origin} 不在授权范围内，未转发凭据与查询体`};
     else if(redirects>=maxRedirects)decision={action:'stop',terminal:'failed',reason:'跳转次数超限'};
     else decision={action:'redirect',next:next.toString(),reason:`网关要求转到同一授权范围内的另一入口（${status}），保持 POST 与查询体重发`};
    }
   }else if(status>=300&&status<400){
    decision={action:'stop',terminal:'failed',reason:`跳转（${status}）按惯例需改用 GET，为保持查询方法已停止，不擅自改方法`};
   }else if(status===408||status===429||status>=500){
    decision=capability.idempotent
     ?{action:'retry',reason:`网关暂时失败（${status}），只读查询允许重发同一查询体`}
     :{action:'stop',terminal:'failed',reason:`网关暂时失败（${status}），查询能力不允许重发`};
   }else{
    decision={action:'stop',terminal:'failed',reason:`网关明确拒绝（${status}），不重发`};
   }
  }else if(result.kind==='cut'){
   if(capability.idempotent)decision={action:'retry',reason:`连接在确认前中断（已发送 ${result.bytesSent}/${staged.bytes} 字节），只读查询允许重发同一完整查询体`};
   else if(result.bytesSent===0)decision={action:'retry',reason:'连接在发送前中断，未产生传输事实，可重发'};
   else decision={action:'stop',terminal:'failed',reason:'传输事实未确认且查询能力不允许重发，停止'};
  }else{
   decision={action:'stop',terminal:'failed',reason:`本地读取暂存体失败：${result.detail}`};
  }
  if(decision.action==='retry'&&attempt>=maxAttempts)
   decision={action:'stop',terminal:'incomplete',reason:`达到最大尝试次数（${maxAttempts}），暂存体保留，可在重启后安全领取`};

  note({phase:'decision',attempt,...decision});
  if(decision.action==='done'){
   await area.release(id);
   note({phase:'cleanup',action:'released',id,reason:'查询已被确认接收，释放暂存体'});
   return {status:'completed',id,digest:staged.digest,bytes:staged.bytes,receipt,audit};
  }
  if(decision.action==='redirect'){current=decision.next;redirects+=1;continue;}
  if(decision.action==='retry'){await sleep(backoffMs*attempt);continue;}
  if(decision.terminal==='incomplete'){
   note({phase:'cleanup',action:'retained',id,reason:'暂存体完整可领取，等待重试或重启后领取'});
   return {status:'incomplete',id,reclaimable:id,digest:staged.digest,bytes:staged.bytes,reason:decision.reason,audit};
  }
  await area.release(id);
  note({phase:'cleanup',action:'released',id,reason:'终态失败，释放暂存体'});
  return {status:'failed',id,reason:decision.reason,audit};
 }
}
