import {request} from 'node:http';

// 单次传输尝试：携带 Expect: 100-continue，等网关明确要求继续后才发送查询体。
// 网关在发送前给出最终响应（如 417）时，查询体一个字节也不会发出。
// 返回传输事实：实际写出的字节数、以及尝试如何结束（响应 / 中断 / 取消 / 本地失败）。
export function sendOnce({url,method='POST',headers={},openBody,bytes,signal,continueTimeoutMs=5000,attemptTimeoutMs=30000}){
 return new Promise((resolve)=>{
  const facts={bytesSent:0};
  let settled=false;let body=null;let responded=false;
  const finish=(outcome)=>{
   if(settled)return;settled=true;
   clearTimeout(continueTimer);clearTimeout(attemptTimer);
   signal?.removeEventListener('abort',onAbort);
   resolve({...facts,...outcome});
  };
  const req=request(url,{method,agent:false,headers:{...headers,'content-length':String(bytes),expect:'100-continue'}});
  const onAbort=()=>{body?.destroy();req.destroy();finish({kind:'cancelled'});};
  if(signal?.aborted){req.destroy();return finish({kind:'cancelled'});}
  signal?.addEventListener('abort',onAbort,{once:true});
  const continueTimer=setTimeout(()=>{req.destroy();finish({kind:'cut',detail:'等待网关继续指令超时'});},continueTimeoutMs);
  const attemptTimer=setTimeout(()=>{body?.destroy();req.destroy();finish({kind:'cut',detail:'等待网关响应超时'});},attemptTimeoutMs);
  req.on('continue',()=>{
   clearTimeout(continueTimer);
   try{body=openBody();}catch(error){req.destroy();return finish({kind:'local-error',detail:error.message});}
   body.on('data',(chunk)=>{
    if(responded||settled)return;
    facts.bytesSent+=chunk.length;
    if(!req.write(chunk))body.pause();
   });
   req.on('drain',()=>{if(!responded&&!settled)body?.resume();});
   body.on('end',()=>{if(!responded&&!settled)req.end();});
   body.on('error',(error)=>{req.destroy();finish({kind:'local-error',detail:error.message});});
  });
  req.on('response',(response)=>{
   responded=true;
   clearTimeout(continueTimer);clearTimeout(attemptTimer);
   body?.destroy();
   const chunks=[];
   response.on('data',(chunk)=>chunks.push(chunk));
   response.on('end',()=>{req.destroy();finish({kind:'responded',status:response.statusCode,headers:response.headers,text:Buffer.concat(chunks).toString('utf8')});});
   response.on('error',(error)=>{req.destroy();finish({kind:'cut',detail:`响应中断：${error.message}`});});
  });
  req.on('error',(error)=>{finish({kind:'cut',detail:error.message,code:error.code});});
  req.flushHeaders();
 });
}
