import crypto from 'node:crypto';
import {setupContacts,saveNames} from './people.js';
const dateCutoff=Date.parse('2026-10-12T00:00:00-03:00');
let initialized;
export async function initialize(pool){if(initialized)return initialized;initialized=(async()=>{
 await pool.query("CREATE TABLE IF NOT EXISTS cg_sync_state(instance text,jid text,last_sync timestamptz,success boolean DEFAULT false,error text,PRIMARY KEY(instance,jid))");
 await pool.query("ALTER TABLE cg_sync_state ADD COLUMN IF NOT EXISTS next_page integer NOT NULL DEFAULT 1");
 await pool.query("ALTER TABLE cg_sync_state ADD COLUMN IF NOT EXISTS backfill_complete boolean NOT NULL DEFAULT false");
 await pool.query("ALTER TABLE cg_sync_state ADD COLUMN IF NOT EXISTS last_page_hash text");
 await pool.query("ALTER TABLE cg_sync_state ADD COLUMN IF NOT EXISTS lock_until timestamptz");
 await pool.query("ALTER TABLE cg_sync_state ADD COLUMN IF NOT EXISTS last_live_sync timestamptz");
 await pool.query("ALTER TABLE cg_messages ADD COLUMN IF NOT EXISTS from_me boolean NOT NULL DEFAULT false");
 await pool.query("ALTER TABLE cg_messages ADD COLUMN IF NOT EXISTS kind text");
 await pool.query("ALTER TABLE cg_messages ADD COLUMN IF NOT EXISTS ingested_at timestamptz DEFAULT now()");
 await setupContacts(pool);
 await pool.query("INSERT INTO cg_sync_state(instance,jid) SELECT instance,jid FROM cg_groups ON CONFLICT DO NOTHING");
 })();try{await initialized}catch(e){initialized=null;throw e}}
const pick=x=>Array.isArray(x)?x:Array.isArray(x?.messages?.records)?x.messages.records:Array.isArray(x?.data?.messages?.records)?x.data.messages.records:Array.isArray(x?.data?.records)?x.data.records:Array.isArray(x?.records)?x.records:[];
function tsOf(x){const v=x.messageTimestamp||x.timestamp||x.createdAt;const n=Number(v?.low??v);const t=Number.isFinite(n)?n>1e12?n:n*1000:Date.parse(v);return Number.isFinite(t)?t:null}
function textOf(m){const msg=m.message?.ephemeralMessage?.message||m.message||{};return msg.conversation||msg.extendedTextMessage?.text||msg.imageMessage?.caption||msg.videoMessage?.caption||msg.documentMessage?.caption||m.text||''}
const fingerprint=ms=>crypto.createHash('sha256').update(ms.map(x=>x.key?.id||x.id||'').join('|')).digest('hex');
export async function store(pool,instance,jid,messages){
 const data=new Map();
 for(const m of messages){
 const id=String(m.key?.id||m.id||''),remote=m.key?.remoteJid||m.remoteJid||jid,t=tsOf(m);
 if(!id||remote!==jid||!t||t<dateCutoff)continue;
 data.set(id,{instance,jid,id,sender:m.key?.participant||m.participant||m.key?.senderLid||m.pushName||'',body:textOf(m),ts:new Date(t).toISOString(),from_me:!!m.key?.fromMe,kind:Object.keys(m.message||{})[0]||'unknown'})
 }
 let saved=0;const all=[...data.values()];
 for(let i=0;i<all.length;i+=120){
 const q=await pool.query("INSERT INTO cg_messages(instance,jid,id,sender,body,ts,from_me,kind) SELECT instance,jid,id,sender,body,ts,from_me,kind FROM jsonb_to_recordset($1::jsonb) AS x(instance text,jid text,id text,sender text,body text,ts timestamptz,from_me boolean,kind text) ON CONFLICT(instance,jid,id) DO NOTHING",[JSON.stringify(all.slice(i,i+120))]);saved+=q.rowCount||0
 }
 if(messages.length)await saveNames(pool,instance,messages);
 return saved
}
const request=(evolution,instance,jid,page)=>evolution('/chat/findMessages/'+encodeURIComponent(instance),{method:'POST',body:{where:{key:{remoteJid:jid}},page,offset:100},timeout:17000});
async function claim(pool,count=3){
const r=await pool.query("WITH selected AS (SELECT instance,jid FROM cg_sync_state WHERE backfill_complete=false AND(lock_until IS NULL OR lock_until<now()) AND (last_sync IS NULL OR last_sync<now()-interval '35 seconds') ORDER BY last_sync ASC NULLS FIRST LIMIT $1 FOR UPDATE SKIP LOCKED) UPDATE cg_sync_state s SET lock_until=now()+interval '90 seconds' FROM selected x WHERE s.instance=x.instance AND s.jid=x.jid RETURNING s.instance,s.jid,s.next_page,s.last_page_hash",[count]);return r.rows
}
async function backfill(pool,evolution,g){
 let page=g.next_page||1,lastHash=g.last_page_hash||null,saved=0,received=0,complete=false,error=null,info='';
 try{
 for(let i=0;i<2;i++){
 const response=await request(evolution,g.instance,g.jid,page),records=pick(response);
 if(!records.length){complete=true;info='Fim do histórico disponibilizado pela API';break}
 const hash=fingerprint(records);if(lastHash===hash){complete=true;info='A API repetiu a mesma página';break}
 saved+=await store(pool,g.instance,g.jid,records);received+=records.length;lastHash=hash;
 const meta=response?.messages||response?.data?.messages||response||{},totalPages=Number(meta.pages||meta.totalPages||0);
 const older=records.filter(m=>{const t=tsOf(m);return t&&t<dateCutoff}).length;
 if(older===records.length||(totalPages>0&&page>=totalPages)){complete=true;info='Chegou ao início do período ou à última página';break}
 page++;
 }
 }catch(e){error=String(e.message||e).slice(0,130)}
 await pool.query("UPDATE cg_sync_state SET next_page=$3,last_page_hash=$4,backfill_complete=$5,last_sync=now(),lock_until=NULL,success=$6,error=$7 WHERE instance=$1 AND jid=$2",[g.instance,g.jid,page,lastHash,complete,!error,error]);
 return{group:g.jid,saved,received,complete,page,error,info}
}
export async function live(pool,evolution,instance,jid){
 await initialize(pool);
 const response=await request(evolution,instance,jid,1),messages=pick(response);
 const saved=await store(pool,instance,jid,messages);
 await pool.query("UPDATE cg_sync_state SET last_live_sync=now() WHERE instance=$1 AND jid=$2",[instance,jid]);
 return{received:messages.length,saved}
}
export async function runBatch(pool,evolution){
 await initialize(pool);const selected=await claim(pool,3);
 const results=await Promise.all(selected.map(g=>backfill(pool,evolution,g)));
 const latest=await pool.query("SELECT instance,jid FROM cg_sync_state WHERE (last_live_sync IS NULL OR last_live_sync<now()-interval '100 seconds') ORDER BY last_live_sync NULLS FIRST LIMIT 1");
 if(latest.rowCount){const g=latest.rows[0];try{results.push({live:true,group:g.jid,...await live(pool,evolution,g.instance,g.jid)})}catch(e){results.push({live:true,group:g.jid,error:String(e.message).slice(0,120)});await pool.query("UPDATE cg_sync_state SET last_live_sync=now() WHERE instance=$1 AND jid=$2",[g.instance,g.jid])}}
 const x=await pool.query("SELECT (SELECT count(*)::int FROM cg_groups) groups,(SELECT count(*)::int FROM cg_messages) messages,(SELECT count(*)::int FROM cg_sync_state WHERE backfill_complete) checked,(SELECT count(*)::int FROM cg_sync_state WHERE error IS NOT NULL) errors");return{result:results,stats:x.rows[0],done:!selected.length}
}
