export const isName=(s)=>typeof s==='string'&&s.trim().length>1&&s.length<160&&!/@(lid|g\.us|s\.whatsapp\.net)/.test(s);
const rows=x=>Array.isArray(x)?x:Array.isArray(x?.data)?x.data:Array.isArray(x?.records)?x.records:[];
const isImageUrl=x=>{try{const u=new URL(x);return u.protocol==='https:'&&x.length<2000?x:null}catch{return null}};
export async function setupContacts(pool){
 await pool.query("CREATE TABLE IF NOT EXISTS cg_people (instance text NOT NULL,jid text NOT NULL,display_name text,avatar_url text,updated_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(instance,jid))");
 await pool.query("ALTER TABLE cg_groups ADD COLUMN IF NOT EXISTS avatar_url text");
}
export async function saveNames(pool,instance,msgs){
 const items=new Map();
 for(const m of msgs){const ids=[m.key?.participant,m.participant,m.key?.senderLid,m.key?.participantAlt,m.senderPn].filter(x=>typeof x==='string'&&x.includes('@'));
  const name=[m.pushName,m.notifyName,m.contactName,m.verifiedBizName].find(isName);
  const avatar=isImageUrl(m.profilePicUrl||m.profilePictureUrl);
  if(!name&&!avatar)continue;
  for(const id of ids){if(!items.has(id))items.set(id,{instance,jid:id,name:name||null,avatar})}
 }
 if(!items.size)return 0;
 const batch=[...items.values()];
 await pool.query("INSERT INTO cg_people(instance,jid,display_name,avatar_url) SELECT instance,jid,name,avatar FROM jsonb_to_recordset($1::jsonb) AS t(instance text,jid text,name text,avatar text) ON CONFLICT(instance,jid) DO UPDATE SET display_name=COALESCE(excluded.display_name,cg_people.display_name),avatar_url=COALESCE(excluded.avatar_url,cg_people.avatar_url),updated_at=now()",[JSON.stringify(batch)]);
 return items.size
}
export async function refreshContacts(pool,evolution,instance){
 const raw=await evolution('/chat/findContacts/'+encodeURIComponent(instance),{method:'POST',body:{},timeout:16000}),cs=rows(raw);
 const items=new Map();
 for(const c of cs){const name=[c.name,c.pushName,c.notify,c.verifiedName,c.verifiedBizName].find(isName),avatar=isImageUrl(c.profilePicUrl||c.profilePictureUrl);
  if(!name&&!avatar)continue;
  const ids=[c.remoteJid,c.id,c.jid,c.lid,c.phoneNumber,c.number].filter(x=>typeof x==='string'&&x.includes('@'));
  for(const id of ids)items.set(id,{instance,jid:id,name:name||null,avatar})
 }
 for(let i=0;i<[...items.values()].length;i+=200){
  const chunk=[...items.values()].slice(i,i+200);
  await pool.query("INSERT INTO cg_people(instance,jid,display_name,avatar_url) SELECT instance,jid,name,avatar FROM jsonb_to_recordset($1::jsonb) AS t(instance text,jid text,name text,avatar text) ON CONFLICT(instance,jid) DO UPDATE SET display_name=COALESCE(excluded.display_name,cg_people.display_name),avatar_url=COALESCE(excluded.avatar_url,cg_people.avatar_url),updated_at=now()",[JSON.stringify(chunk)])
 }
 return{contacts:cs.length,named:items.size}
}
export async function fetchGroupAvatar(pool,evolution,instance,jid){
 const r=await evolution('/chat/fetchProfilePictureUrl/'+encodeURIComponent(instance),{method:'POST',body:{number:jid},timeout:9500});
 const avatar=isImageUrl(r.profilePictureUrl||r.profilePicUrl||r.picture||r.url);
 if(avatar)await pool.query("UPDATE cg_groups SET avatar_url=$3 WHERE instance=$1 AND jid=$2",[instance,jid,avatar]);
 return{avatar_url:avatar}
}
