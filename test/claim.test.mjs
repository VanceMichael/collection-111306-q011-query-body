import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {createHash} from 'node:crypto';
import {mkdtemp,readdir,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {startSource} from '../src/source.mjs';
import {StagingArea} from '../src/staging.mjs';
import {claimQuery} from '../src/claim.mjs';
import {encodeQuery,bodyDigest} from '../src/domain.mjs';

const sha256=bytes=>createHash('sha256').update(bytes).digest('hex');
const many=n=>Array.from({length:n},(_,i)=>`batch-${i}`);
const attemptsOf=result=>result.audit.filter(entry=>entry.phase==='attempt');
const decisionsOf=result=>result.audit.filter(entry=>entry.phase==='decision');

async function tempdir(){return mkdtemp(join(tmpdir(),'claim-'));}

async function startGateway(handler,{continueMode='auto'}={}){
 const received=[];
 const server=createServer((req,res)=>handler(req,res,received));
 server.on('checkContinue',(req,res)=>{
  if(continueMode==='auto')res.writeContinue();
  handler(req,res,received);
 });
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 return {
  url:`http://127.0.0.1:${server.address().port}`,
  received,
  close:async()=>{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));},
 };
}

function collect(req,res,received){
 const chunks=[];
 req.on('data',chunk=>chunks.push(chunk));
 req.on('error',()=>{});
 req.on('end',()=>{
  const bytes=Buffer.concat(chunks);
  const record={method:req.method,path:req.url,bytes:bytes.length,digest:sha256(bytes),authorization:req.headers.authorization??null};
  received.push(record);
  res.writeHead(200,{'content-type':'application/json'});
  res.end(JSON.stringify(record));
 });
}

async function action(source,name){
 const response=await fetch(source.url+'/api/action',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({action:name})});
 assert.equal(response.status,200);
}

test('发送前拒绝接收：417 不重发，查询体一个字节也未发出',async()=>{
 const root=await tempdir();
 const source=await startSource();
 try{
  await action(source,'reject');
  const directory=join(root,'stage');
  const result=await claimQuery({area:new StagingArea(directory),batches:['batch-1','batch-2'],url:source.url+'/query'});
  assert.equal(result.status,'failed');
  assert.match(result.reason,/417/);
  const attempts=attemptsOf(result);
  assert.equal(attempts.length,1);
  assert.equal(attempts[0].bytesSent,0);
  assert.deepEqual(await(await fetch(source.url+'/api/requests')).json(),[]);
  assert.deepEqual(await readdir(directory),[]);
 }finally{await source.close();await rm(root,{recursive:true,force:true});}
});

test('批准跳转：保持 POST 与同一完整查询体重发到授权入口',async()=>{
 const root=await tempdir();
 const source=await startSource();
 try{
  await action(source,'redirect');
  const batches=many(3000);
  const expected=encodeQuery(batches);
  const result=await claimQuery({area:new StagingArea(join(root,'stage')),batches,url:source.url+'/query'});
  assert.equal(result.status,'completed');
  const attempts=attemptsOf(result);
  assert.equal(attempts.length,2);
  assert.ok(attempts.every(entry=>entry.method==='POST'&&entry.digest===bodyDigest(expected)));
  assert.ok(attempts[1].url.endsWith('/query-final'));
  assert.equal(attempts[1].bytesSent,expected.length);
  const records=await(await fetch(source.url+'/api/requests')).json();
  assert.equal(records.length,1);
  assert.deepEqual(records[0],{method:'POST',path:'/query-final',bytes:expected.length,digest:bodyDigest(expected)});
  assert.ok(result.audit.some(entry=>entry.phase==='receipt'&&entry.match===true));
  assert.ok(decisionsOf(result).some(entry=>entry.action==='redirect'&&/保持 POST/.test(entry.reason)));
 }finally{await source.close();await rm(root,{recursive:true,force:true});}
});

test('正常完成：几万条批次号有界暂存，回执与源端记录可对照',async()=>{
 const root=await tempdir();
 const source=await startSource();
 try{
  const batches=many(50000);
  const expected=encodeQuery(batches);
  const result=await claimQuery({area:new StagingArea(join(root,'stage')),batches,url:source.url+'/query'});
  assert.equal(result.status,'completed');
  assert.equal(result.digest,bodyDigest(expected));
  assert.equal(result.bytes,expected.length);
  assert.equal(result.receipt.digest,bodyDigest(expected));
  const records=await(await fetch(source.url+'/api/requests')).json();
  assert.equal(records.length,1);
  assert.equal(records[0].digest,result.digest);
  assert.equal(records[0].bytes,result.bytes);
 }finally{await source.close();await rm(root,{recursive:true,force:true});}
});

test('发送中断：按传输事实重发同一完整查询体',async()=>{
 const root=await tempdir();
 let count=0;
 const gateway=await startGateway((req,res,received)=>{
  if(++count===1){req.once('data',()=>req.socket.destroy());req.on('error',()=>{});return;}
  collect(req,res,received);
 });
 try{
  const batches=many(20000);
  const expected=encodeQuery(batches);
  const result=await claimQuery({area:new StagingArea(join(root,'stage')),batches,url:gateway.url+'/query'});
  assert.equal(result.status,'completed');
  const attempts=attemptsOf(result);
  assert.equal(attempts.length,2);
  assert.equal(attempts[0].outcome,'cut');
  assert.ok(attempts[0].bytesSent>0);
  assert.equal(attempts[1].bytesSent,expected.length);
  assert.ok(attempts.every(entry=>entry.digest===bodyDigest(expected)));
  assert.equal(gateway.received.length,1);
  assert.equal(gateway.received[0].digest,bodyDigest(expected));
  assert.ok(decisionsOf(result).some(entry=>entry.action==='retry'&&/中断/.test(entry.reason)));
 }finally{await gateway.close();await rm(root,{recursive:true,force:true});}
});

test('重试耗尽保留暂存体，进程重启后领取同一查询并完成',async()=>{
 const root=await tempdir();
 let mode='cut';
 const gateway=await startGateway((req,res,received)=>{
  if(mode==='cut'){req.once('data',()=>req.socket.destroy());req.on('error',()=>{});return;}
  collect(req,res,received);
 });
 try{
  const directory=join(root,'stage');
  const first=await claimQuery({area:new StagingArea(directory),batches:many(5000),url:gateway.url+'/query',maxAttempts:2});
  assert.equal(first.status,'incomplete');
  assert.equal(first.reclaimable,first.id);
  assert.equal(attemptsOf(first).length,2);
  const reopened=new StagingArea(directory);
  const sweep=await reopened.sweep();
  assert.deepEqual(sweep.claimable.map(record=>record.id),[first.id]);
  assert.equal(sweep.claimable[0].digest,first.digest);
  mode='accept';
  const second=await claimQuery({area:reopened,id:first.id,url:gateway.url+'/query'});
  assert.equal(second.status,'completed');
  assert.equal(second.digest,first.digest);
  assert.ok(second.audit.some(entry=>entry.phase==='staging'&&entry.action==='reused'));
  assert.equal(gateway.received.length,1);
  assert.equal(gateway.received[0].digest,first.digest);
  assert.equal(gateway.received[0].bytes,first.bytes);
 }finally{await gateway.close();await rm(root,{recursive:true,force:true});}
});

test('跳转目标不在授权范围：不转发凭据与查询体',async()=>{
 const root=await tempdir();
 const target=await startGateway(collect);
 const hop=await startGateway((req,res)=>{
  req.resume();req.on('error',()=>{});
  res.writeHead(307,{location:`${target.url}/query`});
  res.end();
 },{continueMode:'manual'});
 try{
  const result=await claimQuery({
   area:new StagingArea(join(root,'stage')),batches:['batch-1'],url:`${hop.url}/query`,
   credentials:{authorization:'Bearer demo'},
  });
  assert.equal(result.status,'failed');
  assert.match(result.reason,/授权范围/);
  assert.equal(target.received.length,0);
  const attempts=attemptsOf(result);
  assert.equal(attempts.length,1);
  assert.equal(attempts[0].bytesSent,0);
 }finally{await hop.close();await target.close();await rm(root,{recursive:true,force:true});}
});

test('同一授权范围内的另一入口：保持方法重发并携带凭据',async()=>{
 const root=await tempdir();
 const target=await startGateway(collect);
 const hop=await startGateway((req,res)=>{
  req.resume();req.on('error',()=>{});
  res.writeHead(307,{location:`${target.url}/query`});
  res.end();
 },{continueMode:'manual'});
 try{
  const batches=many(3000);
  const expected=encodeQuery(batches);
  const result=await claimQuery({
   area:new StagingArea(join(root,'stage')),batches,url:`${hop.url}/query`,
   credentials:{authorization:'Bearer demo'},
   allowedOrigins:[new URL(hop.url).origin,new URL(target.url).origin],
  });
  assert.equal(result.status,'completed');
  assert.equal(target.received.length,1);
  assert.equal(target.received[0].method,'POST');
  assert.equal(target.received[0].authorization,'Bearer demo');
  assert.equal(target.received[0].digest,bodyDigest(expected));
  assert.equal(target.received[0].bytes,expected.length);
  const attempts=attemptsOf(result);
  assert.equal(attempts.length,2);
  assert.ok(attempts.every(entry=>entry.method==='POST'&&entry.digest===bodyDigest(expected)));
 }finally{await hop.close();await target.close();await rm(root,{recursive:true,force:true});}
});

test('要求改用 GET 的跳转不被擅自接受',async()=>{
 const root=await tempdir();
 const target=await startGateway(collect);
 const hop=await startGateway((req,res)=>{
  req.resume();req.on('error',()=>{});
  res.writeHead(302,{location:`${target.url}/query`});
  res.end();
 },{continueMode:'manual'});
 try{
  const result=await claimQuery({area:new StagingArea(join(root,'stage')),batches:['batch-1'],url:`${hop.url}/query`});
  assert.equal(result.status,'failed');
  assert.match(result.reason,/GET/);
  assert.equal(target.received.length,0);
  assert.equal(attemptsOf(result).length,1);
 }finally{await hop.close();await target.close();await rm(root,{recursive:true,force:true});}
});

test('发送中取消：传输中止，暂存资源清理且重启后无可领取残缺',async()=>{
 const root=await tempdir();
 let sawChunk;
 const firstChunk=new Promise(resolve=>{sawChunk=resolve;});
 let receivedBytes=0;
 const gateway=await startGateway((req,res)=>{
  req.on('error',()=>{});
  req.on('aborted',()=>{});
  req.on('data',chunk=>{receivedBytes+=chunk.length;sawChunk();req.pause();});
 });
 try{
  const batches=many(40000);
  const full=encodeQuery(batches).length;
  const directory=join(root,'stage');
  const controller=new AbortController();
  const pending=claimQuery({area:new StagingArea(directory),batches,url:gateway.url+'/query',signal:controller.signal});
  await firstChunk;
  controller.abort();
  const result=await pending;
  assert.equal(result.status,'cancelled');
  assert.ok(result.audit.some(entry=>entry.phase==='cancelled'));
  assert.ok(result.audit.some(entry=>entry.phase==='cleanup'&&entry.action==='released'));
  assert.deepEqual(await readdir(directory),[]);
  const sweep=await new StagingArea(directory).sweep();
  assert.deepEqual(sweep.claimable,[]);
  assert.ok(receivedBytes<full);
 }finally{await gateway.close();await rm(root,{recursive:true,force:true});}
});

test('暂时失败（503）按查询能力重发，明确拒绝（400）不重发',async()=>{
 const root=await tempdir();
 let count=0;
 const gateway=await startGateway((req,res,received)=>{
  if(++count===1){req.resume();res.writeHead(503);res.end();return;}
  res.writeContinue();
  collect(req,res,received);
 },{continueMode:'manual'});
 try{
  const batches=many(1000);
  const result=await claimQuery({area:new StagingArea(join(root,'stage')),batches,url:gateway.url+'/query'});
  assert.equal(result.status,'completed');
  const attempts=attemptsOf(result);
  assert.equal(attempts.length,2);
  assert.equal(attempts[0].status,503);
  assert.equal(attempts[0].bytesSent,0);
  assert.ok(decisionsOf(result).some(entry=>entry.action==='retry'&&/暂时失败/.test(entry.reason)));
 }finally{await gateway.close();}
 const rejecting=await startGateway((req,res)=>{req.resume();res.writeHead(400);res.end();},{continueMode:'manual'});
 try{
  const result=await claimQuery({area:new StagingArea(join(root,'again')),batches:['batch-1'],url:rejecting.url+'/query'});
  assert.equal(result.status,'failed');
  assert.match(result.reason,/400/);
  assert.equal(attemptsOf(result).length,1);
 }finally{await rejecting.close();await rm(root,{recursive:true,force:true});}
});
