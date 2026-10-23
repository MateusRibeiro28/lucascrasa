import {initialize,live} from "./fast-sync.js";
export async function scheduledPoll(pool,evolution,makeSummary){
 await initialize(pool);
 await pool.query("CREATE TABLE IF NOT EXISTS cg_scheduler_cursor(instance text PRIMARY KEY,next_offset integer NOT NULL DEFAULT 0,lease_until timestamptz,last_run timestamptz,scanned integer NOT NULL DEFAULT 0,added integer NOT NULL DEFAULT 0,last_error text)");
 const instance="lucas_bezerra_rolim";
 await pool.query("INSERT INTO cg_scheduler_cursor(instance) VALUES($1) ON CONFLICT DO NOTHING",[instance]);
 const lease=await pool.query("UPDATE cg_scheduler_cursor SET lease_until=now()+interval '52 seconds' WHERE instance=$1 AND (lease_until IS NULL OR lease_until < now()) RETURNING next_offset",[instance]);
 if(!lease.rowCount)return{busy:true};
 let offset=lease.rows[0].next_offset,scanned=0,added=0,errors=[],changed=[],started=Date.now();
 try{
 const groupRows=await pool.query("SELECT jid FROM cg_groups WHERE instance=$1 ORDER BY jid",[instance]);
 const groups=groupRows.rows.map(g=>g.jid);
 if(!groups.length)return{scanned:0,added:0,reason:"No groups registered"};
 for(let step=0;step<groups.length&&Date.now()-started<35000;step+=4){
   const ids=Array.from({length:Math.min(4,groups.length-step)},(_,i)=>groups[(offset+step+i)%groups.length]);
   const done=await Promise.all(ids.map(async jid=>{
     try{const r=await live(pool,evolution,instance,jid);return{jid,...r}}
     catch(e){return{jid,error:String(e.message||e).slice(0,100)}}
   }));
   scanned+=ids.length;
   for(const d of done){if(d.saved){added+=d.saved;changed.push(d.jid)}if(d.error&&errors.length<4)errors.push(d.error)}
 }
 offset=(offset+scanned)%groups.length;
 if(changed.length)try{await makeSummary(instance,changed[0])}catch(e){if(errors.length<4)errors.push("Resumo: "+String(e.message||e).slice(0,85))}
 await pool.query("UPDATE cg_scheduler_cursor SET next_offset=$2,last_run=now(),scanned=$3,added=$4,last_error=$5,lease_until=NULL WHERE instance=$1",[instance,offset,scanned,added,errors.join("; ")||null]);
 return{scanned,added,total:groups.length,errors}
 }catch(e){
 await pool.query("UPDATE cg_scheduler_cursor SET lease_until=NULL,last_error=$2,last_run=now() WHERE instance=$1",[instance,String(e.message||e).slice(0,160)]);
 throw e
 }
}