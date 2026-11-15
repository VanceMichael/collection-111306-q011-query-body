import {createHash} from 'node:crypto';
import {createReadStream} from 'node:fs';
import {mkdir,open,readdir,readFile,rename,rm,writeFile} from 'node:fs/promises';
import {join} from 'node:path';

// 暂存区：把查询体流式落盘并记录摘要，传输时按文件重放，
// 使同一份查询在有界内存下可以被任意次完整重发。
// 只有「清单 + 内容 + 摘要校验」三者齐全的暂存体才可被领取；
// 残缺（.part）、无清单、无内容或校验失败的暂存体一律不可领取，sweep 时清除。
export class StagingArea{
 constructor(directory){this.directory=directory;}
 paths(id){
  return {
   part:join(this.directory,`${id}.part`),
   bin:join(this.directory,`${id}.bin`),
   manifest:join(this.directory,`${id}.json`),
  };
 }
 async stage(id,chunks,{signal}={}){
  await mkdir(this.directory,{recursive:true});
  const {part,bin,manifest}=this.paths(id);
  const hash=createHash('sha256');
  let bytes=0;
  const handle=await open(part,'wx',0o600);
  try{
   for await(const chunk of chunks){
    if(signal?.aborted){const error=new Error('暂存已取消');error.code='ERR_CLAIM_CANCELLED';throw error;}
    const buffer=Buffer.isBuffer(chunk)?chunk:Buffer.from(chunk);
    hash.update(buffer);bytes+=buffer.length;
    await handle.writeFile(buffer);
   }
   await handle.sync();
   await handle.close();
  }catch(error){
   // 取消或失败：删除未完成的 .part，不留下可被后来请求复用的残缺查询。
   await handle.close().catch(()=>{});
   await rm(part,{force:true});
   throw error;
  }
  await rename(part,bin);
  const record={id,bytes,digest:hash.digest('hex'),createdAt:new Date().toISOString()};
  const temporary=`${manifest}.${process.pid}.tmp`;
  await writeFile(temporary,JSON.stringify(record),{mode:0o600});
  await rename(temporary,manifest);
  const directory=await open(this.directory,'r');
  try{await directory.sync();}finally{await directory.close();}
  return record;
 }
 async verify(record){
  const hash=createHash('sha256');
  let bytes=0;
  try{
   for await(const chunk of createReadStream(this.paths(record.id).bin)){hash.update(chunk);bytes+=chunk.length;}
  }catch{return false;}
  return bytes===record.bytes&&hash.digest('hex')===record.digest;
 }
 // 进程重启后调用：报告哪些暂存体还能安全领取，并清除不可安全领取的部分。
 async sweep(){
  let entries;
  try{entries=await readdir(this.directory);}
  catch(error){if(error.code==='ENOENT')return {claimable:[],removed:[]};throw error;}
  const grouped=new Map();
  for(const entry of entries){
   const match=/^(?<id>.+)\.(?<suffix>part|bin|json)$/.exec(entry);
   if(!match)continue;
   const {id,suffix}=match.groups;
   if(!grouped.has(id))grouped.set(id,{});
   grouped.get(id)[suffix]=true;
  }
  const claimable=[];const removed=[];
  for(const [id,kinds] of grouped){
   const paths=this.paths(id);
   if(kinds.part){await rm(paths.part,{force:true});removed.push({id,reason:'未完成的残缺暂存体，已清除'});}
   if(kinds.json&&kinds.bin){
    let record=null;
    try{record=JSON.parse(await readFile(paths.manifest,'utf8'));}catch{/* 清单不可读按校验失败处理 */}
    if(record&&record.id===id&&await this.verify(record)){claimable.push(record);continue;}
    await rm(paths.bin,{force:true});await rm(paths.manifest,{force:true});
    removed.push({id,reason:'暂存体完整性校验失败，已清除'});
   }else if(kinds.bin){await rm(paths.bin,{force:true});removed.push({id,reason:'缺少清单的暂存体，已清除'});}
   else if(kinds.json){await rm(paths.manifest,{force:true});removed.push({id,reason:'缺少内容的清单，已清除'});}
  }
  return {claimable,removed};
 }
 // 领取一份已暂存的查询体：校验通过才返回清单，否则清除并抛错。
 async claim(id){
  const record=JSON.parse(await readFile(this.paths(id).manifest,'utf8'));
  if(record.id!==id||!(await this.verify(record))){
   await this.release(id);
   throw new Error('暂存体完整性校验失败，不可领取');
  }
  return record;
 }
 openBody(id){return createReadStream(this.paths(id).bin);}
 async release(id){
  const paths=this.paths(id);
  await rm(paths.part,{force:true});
  await rm(paths.bin,{force:true});
  await rm(paths.manifest,{force:true});
 }
}
