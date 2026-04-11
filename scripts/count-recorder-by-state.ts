import 'dotenv/config';
import { createClient } from '@supabase/supabase-js';
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {auth:{persistSession:false}});
async function main() {
  const states = ['OH','IL','IA','NH','MI','WA','TX','AR','NC','NY','PA','IN','WI','NJ','FL','CO','MD','MN'];
  for (const s of states) {
    const { count: recorder } = await db.from('mortgage_records')
      .select('*',{count:'exact',head:true})
      .ilike('source_url', `%${s}%`)
      .not('source_url','like','assessor-sale-%')
      .not('source_url','like','assessor-sale-OH');
    const { count: assessor } = await db.from('mortgage_records')
      .select('*',{count:'exact',head:true})
      .like('source_url', `assessor-sale-${s}`);
    console.log(`${s}: recorder=${(recorder??0).toLocaleString()} | assessor-sale=${(assessor??0).toLocaleString()}`);
  }
}
main().catch(console.error);
