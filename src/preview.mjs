import {resolve} from 'node:path';
import {startSource} from './source.mjs';
import {CaptureStore} from './store.mjs';
import {StagingArea} from './staging.mjs';
import {claimQuery} from './claim.mjs';
import {preview} from './domain.mjs';
const source=await startSource();
try{
 const snapshot=await(await fetch(source.url+'/api/snapshot')).json();
 const observed=preview(snapshot);
 const area=new StagingArea(resolve(process.env.STAGE_PATH??'var/staging'));
 const sweep=await area.sweep();
 const claim=await claimQuery({area,batches:snapshot.seed,url:source.url+snapshot.target});
 const sourceRecords=await(await fetch(source.url+'/api/requests')).json();
 const path=resolve(process.env.CAPTURE_PATH??'var/captures.json');const store=new CaptureStore(path);
 const receipt=await store.append({kind:'query-body',observed,claim:{status:claim.status,digest:claim.digest,bytes:claim.bytes,reason:claim.reason,audit:claim.audit},sourceRecords,at:new Date().toISOString()});
 console.log(JSON.stringify({
  资料源:source.url,接收文件:path,已存记录:receipt.revision,本次预览:observed,
  领取:{状态:claim.status,摘要:claim.digest,字节:claim.bytes,未完成原因:claim.reason,可再领取:claim.reclaimable},
  暂存区:{可安全领取:sweep.claimable.map(record=>record.id),已清除残缺:sweep.removed},
  源端记录:sourceRecords,
 },null,2));
}finally{await source.close();}
