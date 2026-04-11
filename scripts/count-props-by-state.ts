import 'dotenv/config';
import { createClient } from '@supabase/supabase-js';
const db = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_KEY!, {auth:{persistSession:false}});
async function main() {
  const states = ['NY','WI','NC','MD','MN','TX','FL','OH','NJ','CO','IN','AR','NH','IL','IA','PA','MI','WA','VA','CA','AZ','GA','TN'];
  for (const s of states) {
    const { count } = await db.from('properties').select('*',{count:'exact',head:true}).eq('state_code',s);
    const { count: withSale } = await db.from('properties').select('*',{count:'exact',head:true}).eq('state_code',s).gt('last_sale_price',0);
    console.log(`${s}: ${(count??0).toLocaleString()} props | ${(withSale??0).toLocaleString()} with sale price`);
  }
}
main().catch(console.error);
