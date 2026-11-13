import {createHash} from 'node:crypto';
export function encodeQuery(batches) {
 if(!batches.every(value=>typeof value==='string'&&value.length>0))throw new Error('批次号无效');
 return Buffer.from(JSON.stringify({batches}));
}
export function bodyDigest(bytes){return createHash('sha256').update(bytes).digest('hex');}
export function preview(data){const bytes=encodeQuery(data.seed);return {bytes:bytes.length,digest:bodyDigest(bytes)};}
