import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile,mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import * as domain from '../src/domain.mjs';
const data=JSON.parse(await readFile(new URL('../fixtures/source.json',import.meta.url))).data;
const {encodeQuery,encodeQueryChunks,bodyDigest,preview}=domain;
test('资料对象的领域边界和原始预览',async()=>{assert.equal(bodyDigest(encodeQuery(data.seed)).length,64);
assert.throws(()=>encodeQuery(['']));
assert.equal(encodeQuery(['甲']).toString(),'{"batches":["甲"]}');});
test('流式编码与一次性编码字节一致',async()=>{
 const chunks=[];for await(const chunk of encodeQueryChunks(data.seed))chunks.push(Buffer.from(chunk));
 assert.ok(Buffer.concat(chunks).equals(encodeQuery(data.seed)));
 const empty=[];for await(const chunk of encodeQueryChunks([]))empty.push(chunk);
 assert.equal(empty.join(''),'{"batches":[]}');
 await assert.rejects(async()=>{for await(const chunk of encodeQueryChunks(['']))void chunk;},/批次号无效/);
});
