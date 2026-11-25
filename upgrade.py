import os,json,asyncio,time
import asyncpg,websockets,httpx
pool=None
client=None
market={}
async def init():
 global pool,client
 client=httpx.AsyncClient(timeout=15)
 pool=await asyncpg.create_pool(os.environ['DATABASE_URL'],min_size=1,max_size=3)
 await pool.execute('CREATE TABLE IF NOT EXISTS okx_ai_events (id bigserial PRIMARY KEY, created_at timestamptz DEFAULT now(), payload jsonb NOT NULL)')
async def record(payload):
 await pool.execute('INSERT INTO okx_ai_events(payload) VALUES($1::jsonb)',json.dumps(payload,ensure_ascii=False))
async def feed():
 while True:
  try:
   async with websockets.connect('wss://ws.okx.com:8443/ws/v5/public',ping_interval=15) as ws:
    await ws.send(json.dumps({'op':'subscribe','args':[{'channel':'tickers','instId':i} for i in ('BTC-USDT','BTC-USDT-SWAP')]}))
    async for raw in ws:
     d=json.loads(raw)
     for row in d.get('data',[]): market[row['instId']]=(time.monotonic(),row)
  except asyncio.CancelledError: raise
  except Exception: market.clear(); await asyncio.sleep(2)
async def telegram(state,cycle,okx,lock):
 token=os.environ['TELEGRAM_BOT_TOKEN']; admin=int(os.environ['TELEGRAM_ADMIN_ID']); offset=0
 async with httpx.AsyncClient(timeout=40) as c:
  async def call(method,**kw):
   r=await c.post('https://api.telegram.org/bot'+token+'/'+method,json=kw); r.raise_for_status(); d=r.json()
   if not d.get('ok'): raise RuntimeError('Telegram API error')
   return d['result']
  while True:
   try:
    updates=await call('getUpdates',offset=offset,timeout=25,allowed_updates=['message'])
    for u in updates:
     offset=u['update_id']+1; m=u.get('message',{}); chat=m.get('chat',{})
     if chat.get('type')!='private' or m.get('from',{}).get('id')!=admin: continue
     cmd=m.get('text','').split(); reply='الأوامر: /status /spot /swap /demo /live /analyze /trade /stop /balance /positions /history'
     try:
      if not cmd: continue
      a=cmd[0]
      if a=='/stop': state['running']=False; reply='تم إيقاف فتح صفقات جديدة. المراكز المفتوحة لا تغلق.'
      elif a=='/status': reply=json.dumps({k:v for k,v in state.items() if k!='events'},ensure_ascii=False)
      elif a in ('/spot','/swap','/demo','/live'):
       async with lock:
        state['running']=False
        if a=='/live' and os.getenv('ALLOW_LIVE')!='yes': raise ValueError('التداول الحقيقي غير مسموح')
        if a in ('/spot','/swap'): state['instrument']='BTC-USDT' if a=='/spot' else 'BTC-USDT-SWAP'
        else: state['mode']=a[1:]
       reply='تم تغيير الإعداد وإيقاف التداول'
      elif a in ('/analyze','/trade'):
       async with lock:
        state['running']=a=='/trade' and (state['mode']=='demo' or cmd[1:]==['CONFIRM'])
        if a=='/trade' and not state['running']: raise ValueError('للتأكيد الحقيقي: /trade CONFIRM')
        try: await cycle()
        finally: state['running']=False
        reply=json.dumps(state['last'],ensure_ascii=False)
      elif a in ('/balance','/positions'):
       async with lock: reply=json.dumps(await okx('GET','/api/v5/account/'+a[1:],private=True),ensure_ascii=False)
      elif a=='/history': reply='\n'.join(str(r['payload']) for r in await pool.fetch('SELECT payload FROM okx_ai_events ORDER BY id DESC LIMIT 5'))
     except Exception as e: state['running']=False; reply='تعذر التنفيذ: '+type(e).__name__
     await call('sendMessage',chat_id=chat['id'],text=reply[:3900])
   except asyncio.CancelledError: raise
   except Exception: await asyncio.sleep(3)
