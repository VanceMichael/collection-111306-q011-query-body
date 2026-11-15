import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,readdir,rm,writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {StagingArea} from '../src/staging.mjs';
import {encodeQuery,encodeQueryChunks,bodyDigest} from '../src/domain.mjs';

async function collect(stream){const chunks=[];for await(const chunk of stream)chunks.push(chunk);return Buffer.concat(chunks);}

test('流式暂存与一次性编码字节一致，重启后领取内容相同',async()=>{
 const root=await mkdtemp(join(tmpdir(),'staging-'));
 try{
  const batches=Array.from({length:50000},(_,i)=>`batch-${i}`);
  const expected=encodeQuery(batches);
  const area=new StagingArea(join(root,'stage'));
  const record=await area.stage('q1',encodeQueryChunks(batches));
  assert.equal(record.bytes,expected.length);
  assert.equal(record.digest,bodyDigest(expected));
  const reopened=new StagingArea(join(root,'stage'));
  const claimed=await reopened.claim('q1');
  assert.equal(claimed.digest,record.digest);
  assert.ok((await collect(reopened.openBody('q1'))).equals(expected));
 }finally{await rm(root,{recursive:true,force:true});}
});

test('暂存中取消：不留下可被后来请求复用的残缺查询',async()=>{
 const root=await mkdtemp(join(tmpdir(),'staging-'));
 try{
  const directory=join(root,'stage');
  const area=new StagingArea(directory);
  const controller=new AbortController();
  async function* slow(){
   yield '{"batches":[';
   await new Promise(resolve=>setTimeout(resolve,20));
   controller.abort();
   yield '"batch-x"]}';
  }
  await assert.rejects(area.stage('q2',slow(),{signal:controller.signal}),/取消/);
  assert.deepEqual(await readdir(directory),[]);
  const sweep=await new StagingArea(directory).sweep();
  assert.deepEqual(sweep.claimable,[]);
  assert.deepEqual(sweep.removed,[]);
 }finally{await rm(root,{recursive:true,force:true});}
});

test('进程重启后只有校验完整的暂存体可安全领取',async()=>{
 const root=await mkdtemp(join(tmpdir(),'staging-'));
 try{
  const directory=join(root,'stage');
  const area=new StagingArea(directory);
  await area.stage('good',encodeQueryChunks(['batch-a','batch-b']));
  await writeFile(join(directory,'broken.part'),'{"batches":["batch-x');
  await writeFile(join(directory,'orphan.bin'),'{}');
  await writeFile(join(directory,'lost.json'),JSON.stringify({id:'lost',bytes:2,digest:'0'.repeat(64)}));
  await writeFile(join(directory,'corrupt.bin'),'{}');
  await writeFile(join(directory,'corrupt.json'),JSON.stringify({id:'corrupt',bytes:2,digest:'0'.repeat(64)}));
  const reopened=new StagingArea(directory);
  const sweep=await reopened.sweep();
  assert.deepEqual(sweep.claimable.map(record=>record.id),['good']);
  assert.deepEqual(sweep.removed.map(record=>record.id).sort(),['broken','corrupt','lost','orphan']);
  await assert.rejects(reopened.claim('corrupt'));
  assert.deepEqual((await readdir(directory)).sort(),['good.bin','good.json']);
 }finally{await rm(root,{recursive:true,force:true});}
});
