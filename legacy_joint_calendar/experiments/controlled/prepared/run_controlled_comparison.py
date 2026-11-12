from __future__ import annotations
from pathlib import Path
import argparse,hashlib,json,platform,shutil,subprocess,sys,time

ROOT=Path(__file__).resolve().parents[3];RUN="controlled_comparison_20261022"
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def dump(p,x):p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(x,indent=2)+"\n",encoding="utf-8")

def worker(folder):
 sys.path.insert(0,str(folder/"prepared"));from controlled_baselines import all_methods
 contract=json.loads((folder/"prepared"/"CONTRACT.json").read_text());rows=[]
 for n,case in enumerate(contract["cases"],1):
  begin=time.perf_counter();methods=all_methods(case["configuration"],case["initial"],contract["random_seeds"]);out={"case":case,"methods":methods,"wall_ms":(time.perf_counter()-begin)*1000};dump(folder/"cases"/case["key"] /"RESULT.json",out)
  row={"key":case["key"],"source":case["source_case"],"factor":case["factor"],"level":case["level"],"wall_ms":out["wall_ms"]}
  for name,item in methods.items():row[name]=item if name=="Random16MedianCost" else item["cost"]
  rows.append(row);print(json.dumps({"done":n,"total":len(contract["cases"]),"key":case["key"],"JCR":row["JCR"],"CRP":row["CRP"],"RCRP":row["RCRP"]}),flush=True)
 dump(folder/"RESULTS.json",{"state":"FINISHED_AWAITING_INDEPENDENT_AUDIT","cases":rows})

def main():
 ap=argparse.ArgumentParser();ap.add_argument("--worker",action="store_true");a=ap.parse_args();folder=ROOT/"raw_results"/RUN
 if a.worker:worker(folder);return
 if folder.exists():raise FileExistsError(folder)
 (folder/"prepared").mkdir(parents=True);(folder/"cases").mkdir();sources=[]
 for p in sorted((ROOT/"raw_results"/"rollout_crp_20261022"/"cases").glob("design_*/RESULT.json")):
  d=json.loads(p.read_text());sources.append({"key":d["key"],"configuration":d["configuration"],"initial":d["initial"],"path":str(p.relative_to(ROOT)).replace("\\","/"),"sha256":sha(p)})
 sweeps={"freshness_rho":[0.75,0.80,0.85,0.90],"source_weight_scale":[0.25,1.0,4.0,16.0],"delivery_rate_scale":[1.0,2.0,4.0,8.0],"lag_addition":[0,4,8,16]};cases=[]
 for src in sources:
  for factor,levels in sweeps.items():
   for level in levels:
    cfg=json.loads(json.dumps(src["configuration"]));
    if factor=="freshness_rho":cfg["rho"]=level
    elif factor=="source_weight_scale":cfg["cpu_weight"]*=level
    elif factor=="delivery_rate_scale":cfg["rate"]*=level
    elif factor=="lag_addition":cfg["lags"]=[x+level for x in cfg["lags"]]
    key=f"{src['key']}__{factor}__{str(level).replace('.','p')}";cases.append({"key":key,"source_case":src["key"],"factor":factor,"level":level,"configuration":cfg,"initial":src["initial"],"source_path":src["path"],"source_sha256":src["sha256"]})
 contract={"id":RUN,"frozen_before_formal_run":True,"design":"One-factor-at-a-time; every sweep reuses each of twelve calendars and changes only its named factor. All directions weakly preserve input feasibility: rho never exceeds 0.9, rate and lag only increase, and positive weights do not alter feasibility.","sweeps":sweeps,"case_count":len(cases),"random_seeds":[91001+i for i in range(16)],"methods":{"Base":"unchanged projected input","CRP":"byte-ordered sequential safe deletion","FreshRatio":"D01 cost-saving per one-plus aggregate freshness loss; QoDA-inspired quality/cost specialization, not QoDA reproduction","LPOrder":"root-LP recipient fractions followed by checked physical deletion","ShareFirst":"lexicographically minimize shared source cost then true cost; sharing-first specialization, not iShare or BIGSHARED reproduction","DeliveryFirst":"lexicographically minimize destination byte cost then true cost","Random16":"sixteen frozen random deletion orders","RCRP":"one-step rollout over CRP","JCR":"exact same-domain MILP oracle"},"literature_boundary":"Tempura and Agamotto change incremental plans or execution times and are not implemented as same-domain baselines. iShare, BIGSHARED, and QoDA are credited as motivation only; named specializations are ours and use identical D01 inputs, feasibility checks, and objective accounting.","primary_metrics":["true declared cost","relative gap to JCR using positive JCR denominator","optimal-match rate","planner time described without speed claim"],"cases":cases}
 dump(folder/"prepared"/"CONTRACT.json",contract)
 for name in ["controlled_baselines.py","contract_prune.py","joint_calendar.py","rollout_calendar.py","run_controlled_comparison.py"]:shutil.copyfile(ROOT/"src"/name,folder/"prepared"/name)
 cmd=[sys.executable,"-I","-B","-X","utf8",str(folder/"prepared"/"run_controlled_comparison.py"),"--worker"];dump(folder/"COMMAND_ENVIRONMENT_BEFORE_RUN.json",{"command":cmd,"python":sys.version,"platform":platform.platform(),"frozen":[{"path":str(p.relative_to(folder)).replace("\\","/"),"sha256":sha(p)} for p in sorted((folder/"prepared").iterdir())]})
 with (folder/"stdout.log").open("wb") as out,(folder/"stderr.log").open("wb") as err:done=subprocess.run(cmd,cwd=ROOT,stdout=out,stderr=err)
 dump(folder/"RECEIPT.json",{"exit_code":done.returncode,"state":"FINISHED_AWAITING_AUDIT" if done.returncode==0 else "FAILED_PRESERVED"});dump(folder/"LOCAL_FILE_MANIFEST.json",[{"path":str(p.relative_to(folder)).replace("\\","/"),"bytes":p.stat().st_size,"sha256":sha(p)} for p in sorted(folder.rglob("*")) if p.is_file()]);print(json.dumps({"run":RUN,"exit_code":done.returncode,"cases":len(cases)}))
 if done.returncode:print((folder/"stderr.log").read_text(errors="replace")[-6000:]);raise SystemExit(done.returncode)
if __name__=="__main__":main()
