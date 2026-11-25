import upgrade as u
import os, asyncio, json, time, hmac, hashlib, base64, secrets
from datetime import datetime, timezone
from decimal import Decimal
import httpx
from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager

state={'running':False,'mode':'demo','instrument':'BTC-USDT','last':None,'error':None,'events':[]}
lock=asyncio.Lock()
security=HTTPBasic()
def auth(c:HTTPBasicCredentials=Depends(security)):
    p=os.getenv('ADMIN_PASSWORD','')
    if not p or not secrets.compare_digest(c.password,p) or c.username!='admin':
        raise HTTPException(401,headers={'WWW-Authenticate':'Basic'})
def log(x):
    state['events']=(state['events']+[{'time':datetime.now(timezone.utc).isoformat(),'event':x}])[-50:]
async def okx(method,path,body=None,private=False):
    raw=json.dumps(body,separators=(',',':')) if body is not None else ''
    headers={'Content-Type':'application/json'}
    if private:
        key,secret,phrase=[os.environ[k] for k in ('OKX_API_KEY','OKX_API_SECRET','OKX_PASSPHRASE')]
        ts=datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')
        sig=base64.b64encode(hmac.new(secret.encode(),(ts+method+path+raw).encode(),hashlib.sha256).digest()).decode()
        headers.update({'OK-ACCESS-KEY':key,'OK-ACCESS-SIGN':sig,'OK-ACCESS-TIMESTAMP':ts,'OK-ACCESS-PASSPHRASE':phrase})
        if state['mode']=='demo': headers['x-simulated-trading']='1'
    if True:
        c=u.client
        r=await c.request(method,'https://www.okx.com'+path,headers=headers,content=raw or None)
        r.raise_for_status(); d=r.json()
    if d.get('code')!='0': raise RuntimeError('OKX: '+str(d.get('code'))+' '+str(d.get('msg'))[:200])
    return d['data']
async def decide(candles,inst):
    if True:
        c=u.client
        r=await c.post('https://api.groq.com/openai/v1/chat/completions',headers={'Authorization':'Bearer '+os.environ['GROQ_API_KEY']},json={'model':'openai/gpt-oss-120b','temperature':0,'reasoning_effort':'low','max_completion_tokens':512,'response_format':{'type':'json_object'},'messages':[{'role':'system','content':'Analyze market data. Return JSON with action BUY, SELL or HOLD and reason. Prefer HOLD when uncertain. No other keys required.'},{'role':'user','content':json.dumps({'instrument':inst,'closed_candles_oldest_first':candles})}]})
        r.raise_for_status(); d=json.loads(r.json()['choices'][0]['message']['content'])
    if d.get('action') not in ('BUY','SELL','HOLD'): raise ValueError('Invalid AI decision')
    return {'action':d['action'],'reason':str(d.get('reason',''))[:500]}
async def cycle():
    inst=state['instrument']; swap=inst.endswith('-SWAP')
    candles=await okx('GET','/api/v5/market/candles?instId='+inst+'&bar=1m&limit=100')
    closed=[x[:6] for x in reversed(candles) if x[8]=='1']
    if len(closed)<30: raise RuntimeError('Insufficient market data')
    start=time.monotonic(); d=await decide(closed[-40:],inst); d['analysis_ms']=round((time.monotonic()-start)*1000); state['last']=d; log(d); await u.record({'instrument':inst,'mode':state['mode'],'decision':d})
    if d['action']=='HOLD' or not state['running']: return
    if inst not in u.market or time.monotonic()-u.market[inst][0]>5: raise RuntimeError('Stale market feed')
    # Explicitly bounded orders; never retry an ambiguous submission.
    if state['mode']=='live' and os.getenv('ALLOW_LIVE')!='yes': raise RuntimeError('Live trading disabled')
    if await okx('GET','/api/v5/trade/orders-pending?instId='+inst,private=True): return
    price=Decimal(closed[-1][4]); cap=Decimal(os.getenv('MAX_ORDER_USDT','10'))
    if not 0<cap<=100: raise ValueError('MAX_ORDER_USDT must be 0..100')
    side=d['action'].lower(); order={'instId':inst,'side':side,'ordType':'market','clOrdId':secrets.token_hex(12)}
    if swap:
        # Only open one isolated position at a time; attached exchange-side exits.
        positions=await okx('GET','/api/v5/account/positions?instId='+inst,private=True)
        if any(Decimal(p['pos'])!=0 for p in positions): return
        info=(await okx('GET','/api/v5/public/instruments?instType=SWAP&instId='+inst))[0]
        if info['ctValCcy']!='BTC': raise RuntimeError('Unsupported contract denomination')
        qty=Decimal(info['minSz']); notional=qty*Decimal(info['ctVal'])*price
        if notional>cap: return
        await okx('POST','/api/v5/account/set-leverage',{'instId':inst,'lever':'1','mgnMode':'isolated'},True)
        order.update(tdMode='isolated',sz=str(qty))
        tick=Decimal(info['tickSz']); sl=price*(Decimal('.99') if side=='buy' else Decimal('1.01')); tp=price*(Decimal('1.02') if side=='buy' else Decimal('.98'))
        order['attachAlgoOrds']=[{'slTriggerPx':str(sl.quantize(tick)),'slOrdPx':'-1','tpTriggerPx':str(tp.quantize(tick)),'tpOrdPx':'-1'}]
    else:
        balances=await okx('GET','/api/v5/account/balance?ccy=BTC,USDT',private=True)
        b={x['ccy']:Decimal(x.get('availBal') or '0') for a in balances for x in a['details']}
        # Avoid repeated accumulation. Sell only up to the configured cap.
        if side=='buy':
            if b.get('BTC',0)*price>=cap or b.get('USDT',0)<cap: return
            order.update(tdMode='cash',sz=str(cap),tgtCcy='quote_ccy')
        else:
            qty=min(b.get('BTC',Decimal(0)),cap/price).quantize(Decimal('.00000001'),rounding='ROUND_DOWN')
            if qty<=0: return
            order.update(tdMode='cash',sz=str(qty),tgtCcy='base_ccy')
    # One order per manual arming: prevents repeated exposure and restart duplication.
    state['running']=False
    await u.record({'submission':order,'mode':state['mode']})
    result=await okx('POST','/api/v5/trade/order',order,True)
    await u.record({'result':result})
    log({'order_result':result})
    if any(x.get('sCode')!='0' for x in result): raise RuntimeError('Order rejected; inspect event log')
async def worker():
    while True:
        if state['running']:
            async with lock:
                try: await cycle(); state['error']=None
                except Exception as e: state['error']=type(e).__name__+': '+str(e)[:250]; state['running']=False
        await asyncio.sleep(1)
@asynccontextmanager
async def lifespan(app):
    await u.init()
    tasks=[asyncio.create_task(u.feed()),asyncio.create_task(u.telegram(state,cycle,okx,lock)),asyncio.create_task(worker())]
    try: yield
    finally:
        for task in tasks: task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        await u.client.aclose(); await u.pool.close()
app=FastAPI(lifespan=lifespan,docs_url=None,redoc_url=None)
@app.get('/health')
def health(): return {'ok':True}
@app.get('/status',dependencies=[Depends(auth)])
def status(): return state
@app.post('/control/{action}',dependencies=[Depends(auth)])
async def control(action:str,mode:str='demo',instrument:str='BTC-USDT'):
    if action=='stop': state['running']=False; return state
    async with lock:
        if action!='start' or mode not in ('demo','live') or instrument not in ('BTC-USDT','BTC-USDT-SWAP'): raise HTTPException(400)
        if mode=='live' and os.getenv('ALLOW_LIVE')!='yes': raise HTTPException(403,'Live disabled')
        if any(not os.getenv(k) for k in ('OKX_API_KEY','OKX_API_SECRET','OKX_PASSPHRASE','GROQ_API_KEY')): raise HTTPException(400,'Missing credentials')
        state.update(running=True,mode=mode,instrument=instrument,error=None); return state
@app.get('/',response_class=HTMLResponse,dependencies=[Depends(auth)])
def home(): return '''<!doctype html><html lang="ar" dir="rtl"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>OKX AI</title><style>body{background:#101820;color:#eee;font:18px system-ui;max-width:760px;margin:auto;padding:24px}button,select{padding:12px;margin:6px;border-radius:8px}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#202c36;padding:16px;direction:ltr}</style><h1>OKX AI | لوحة التداول</h1><p>تسليح صفقة واحدة فقط. الإيقاف لا يغلق المراكز المفتوحة.</p><select id="m"><option value="demo">تجريبي</option><option value="live">حقيقي</option></select><select id="i"><option>BTC-USDT</option><option>BTC-USDT-SWAP</option></select><button onclick="go('start')">بدء التحليل</button><button onclick="go('stop')">إيقاف</button><pre id="s"></pre><script>async function refresh(){s.textContent=JSON.stringify(await (await fetch('/status')).json(),null,2)}async function go(a){if(a==='start'&&m.value==='live'&&!confirm('تأكيد إرسال أمر حقيقي واحد؟'))return;let r=await fetch('/control/'+a+'?mode='+m.value+'&instrument='+i.value,{method:'POST'});if(!r.ok)alert(await r.text());refresh()}refresh();setInterval(refresh,3000)</script></html>'''
