import {createHash} from 'node:crypto';
export function encodeQuery(batches) {
 if(!batches.every(value=>typeof value==='string'&&value.length>0))throw new Error('批次号无效');
 return Buffer.from(JSON.stringify({batches}));
}
export async function* encodeQueryChunks(batches,{flushEvery=1024}={}){
 // 与 encodeQuery 字节一致的流式编码：几万条批次号不必一次进入内存。
 yield '{"batches":[';let first=true;let pending=0;let buffer='';
 for await(const value of batches){
  if(typeof value!=='string'||value.length===0)throw new Error('批次号无效');
  buffer+=(first?'':',')+JSON.stringify(value);first=false;
  if(++pending>=flushEvery){yield buffer;buffer='';pending=0;}
 }
 if(buffer)yield buffer;
 yield ']}';
}
export function bodyDigest(bytes){return createHash('sha256').update(bytes).digest('hex');}
export function preview(data){const bytes=encodeQuery(data.seed);return {bytes:bytes.length,digest:bodyDigest(bytes)};}
