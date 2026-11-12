"""D01-CRP: finite,contract-preserving pruning of an unissued calendar.
Keeps source epochs and command starts fixed. No global approximation claim.
"""
import copy,math,time
def validate(cfg,plan):
 errors=[]
 for kind in ['build','pub']:
  prior=-1
  for q in sorted((q for q in plan if q['kind']==kind),key=lambda q:q['start']):
   if any(type(q[x]) is not int for x in ['start','end','epoch']) or q['start']<max(prior,cfg['startup']) or q['end']<=q['start'] or q['end']>=cfg['T']:errors.append('resource_or_time')
   prior=q['end']
 for q in plan:
  if q['kind']=='build':
   if q['epoch']!=q['start'] or q['epoch'] not in cfg['captures'] or q['end']-q['start']!=cfg['prefix']+sum(cfg['suffix'][v] for v in q['views']):errors.append('build_domain')
  else:
   if not q['recipients'] or len(set(q['recipients']))!=len(q['recipients']) or any(cfg['views'][c]!=q['view'] for c in q['recipients']):errors.append('recipient_domain')
   if q['end']-q['start']!=max(1,math.ceil(cfg['upper'][q['view']]*len(q['recipients'])/cfg['rate'])):errors.append('delivery_duration')
   if not any(b['kind']=='build' and b['epoch']==q['epoch'] and q['view'] in b['views'] and b['end']<=q['start'] for b in plan):errors.append('exact_payload_unavailable')
 return errors
def fresh_counts(cfg,plan):
 result=[]
 for c,lag in enumerate(cfg['lags']):
  ranges=[(cfg['warm'],min(cfg['T']-1,cfg['initial'][c]+lag))]
  ranges.extend((max(cfg['warm'],q['end']),min(cfg['T']-1,q['epoch']+lag)) for q in plan if q['kind']=='pub' and c in q['recipients'])
  intervals=sorted((a,b) for a,b in ranges if a<=b);total=0;left=right=None
  for a,b in intervals:
   if left is None:left,right=a,b
   elif a<=right+1:right=max(right,b)
   else:total+=right-left+1;left,right=a,b
  if left is not None:total+=right-left+1
  result.append(total)
 return result
def budget(cfg):return math.floor((cfg['T']-cfg['warm'])*(1-cfg['rho'])+1e-9)
def feasible(cfg,plan):return min(fresh_counts(cfg,plan))>=cfg['T']-cfg['warm']-budget(cfg)
def cost(cfg,plan):return sum(cfg['cpu_weight']*(q['end']-q['start']) if q['kind']=='build' else cfg['byte_weight']*cfg['upper'][q['view']]*len(q['recipients']) for q in plan)
def shrink(cfg,plan):
 plan=copy.deepcopy(plan);pubs=[q for q in plan if q['kind']=='pub' and q['recipients']];out=list(pubs)
 for q in plan:
  if q['kind']!='build':continue
  need=sorted({p['view'] for p in pubs if p['epoch']==q['epoch'] and p['view'] in q['views']})
  if need:q['views']=need;q['end']=q['start']+cfg['prefix']+sum(cfg['suffix'][v] for v in need);out.append(q)
 for q in pubs:q['end']=q['start']+max(1,math.ceil(cfg['upper'][q['view']]*len(q['recipients'])/cfg['rate']))
 return sorted(out,key=lambda q:(q['start'],q['kind'],q.get('view',-1)))
def contract_prune(cfg,initial):
 begin=time.perf_counter();plan=shrink(cfg,initial)
 if validate(cfg,initial) or validate(cfg,plan) or not feasible(cfg,plan):raise ValueError('pruning requires a feasible initial calendar')
 decisions=[];attempts=0;removed=0
 # Revisit rejected edges after each acceptance: shortening another group may
 # make a previously rejected removal safe. Every acceptance removes one edge.
 while True:
  candidates=sorted([(cfg['upper'][q['view']],q['start'],q['view'],q['epoch'],c) for q in plan if q['kind']=='pub' for c in q['recipients']],reverse=True);accepted=False
  for _,s,v,e,c in candidates:
   trial=copy.deepcopy(plan);q=next(q for q in trial if q['kind']=='pub' and q['start']==s and q['view']==v and q['epoch']==e and c in q['recipients']);q['recipients'].remove(c);trial=shrink(cfg,trial);counts=fresh_counts(cfg,trial);attempts+=1;ok=min(counts)>=cfg['T']-cfg['warm']-budget(cfg)
   decisions.append(dict(iteration=attempts,removed_edge=[v,e,s,c],accepted=ok,counts=counts,cost_before=cost(cfg,plan),cost_trial=cost(cfg,trial)))
   if ok:
    assert cost(cfg,trial)<=cost(cfg,plan)+1e-9;plan=trial;accepted=True;removed+=1;break
  if not accepted:break
 return dict(plan=plan,initial_cost=cost(cfg,initial),projected_initial_cost=cost(cfg,shrink(cfg,initial)),final_cost=cost(cfg,plan),counts=fresh_counts(cfg,plan),decisions=decisions,accepted_removals=removed,attempted_removals=attempts,wall_ms=(time.perf_counter()-begin)*1000,scope='Initial calendar before any command is issued;fixed capture/start times,constructed bounds,not global optimum or online adaptation')
def cycle_seed(cfg,family):
 best=None;V=len(cfg['suffix']);T=cfg['T'];allcs=lambda v:[c for c,x in enumerate(cfg['views']) if x==v]
 d=[max(1,math.ceil(cfg['upper'][v]*len(allcs(v))/cfg['rate'])) for v in range(V)]
 if family=='FullOnly':base=max(cfg['prefix']+sum(cfg['suffix']),sum(d))
 elif family=='ViewOnly':base=max(sum(cfg['prefix']+x for x in cfg['suffix']),sum(d))
 else:raise ValueError('restricted seed family required')
 grid=cfg['captures'][1]-cfg['captures'][0]
 for period in range(math.ceil(base/grid)*grid,T+1,grid):
  for phase in cfg['captures']:
   if phase>period:break
   plan=[];network_end=0
   for start in range(phase,T,period):
    if family=='FullOnly':
     end=start+cfg['prefix']+sum(cfg['suffix'])
     if end>=T:continue
     plan.append(dict(kind='build',epoch=start,start=start,end=end,views=list(range(V))))
     for v in range(V):
      at=max(end,network_end)
      if at+d[v]<T:plan.append(dict(kind='pub',view=v,epoch=start,start=at,end=at+d[v],recipients=allcs(v)));network_end=at+d[v]
    else:
     at=start
     for v in range(V):
      end=at+cfg['prefix']+cfg['suffix'][v]
      if at in cfg['captures'] and end<T:
       plan.append(dict(kind='build',epoch=at,start=at,end=end,views=[v]));send=max(end,network_end)
       if send+d[v]<T:plan.append(dict(kind='pub',view=v,epoch=at,start=send,end=send+d[v],recipients=allcs(v)));network_end=send+d[v]
      at=end
   if feasible(cfg,plan) and (best is None or cost(cfg,plan)<best['objective']):best=dict(plan=sorted(plan,key=lambda q:(q['start'],q['kind'],q.get('view',-1))),objective=cost(cfg,plan),period=period,phase=phase)
 return best
