from __future__ import annotations
from pathlib import Path
import argparse, hashlib, json, platform, shutil, subprocess, sys, time

ROOT=Path(__file__).resolve().parents[3]; RUN="heldout_same_domain_20261022"
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def dump(p,x):p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(x,indent=2)+"\n",encoding="utf-8")

def worker(folder):
 sys.path.insert(0,str(folder/"prepared"));from controlled_baselines import all_methods
 c=json.loads((folder/"prepared"/"CONTRACT.json").read_text());rows=[]
 for n,case in enumerate(c["cases"],1):
  begin=time.perf_counter();m=all_methods(case["configuration"],case["initial"],c["random_seeds"]);out={"case":case,"methods":m,"wall_ms":(time.perf_counter()-begin)*1000};dump(folder/"cases"/case["key"]/"RESULT.json",out)
  row={"key":case["key"],"reference_optimum":case["reference_optimum"],"wall_ms":out["wall_ms"]}
  for name,item in m.items():row[name]=item if name=="Random16MedianCost" else item["cost"]
  rows.append(row);print(json.dumps({"done":n,"total":len(c["cases"]),"key":case["key"],"JCR":row["JCR"],"reference":row["reference_optimum"]}),flush=True)
 dump(folder/"RESULTS.json",{"state":"FINISHED_AWAITING_INDEPENDENT_AUDIT","cases":rows})

def main():
 ap=argparse.ArgumentParser();ap.add_argument("--worker",action="store_true");a=ap.parse_args();folder=ROOT/"raw_results"/RUN
 if a.worker:worker(folder);return
 if folder.exists():raise FileExistsError(folder)
 (folder/"prepared").mkdir(parents=True);(folder/"cases").mkdir();cases=[]
 for p in sorted((ROOT/"raw_results"/"rollout_crp_20261022"/"cases").glob("holdout_*/RESULT.json")):
  d=json.loads(p.read_text());cases.append({"key":d["key"],"configuration":d["configuration"],"initial":d["initial"],"reference_optimum":d["reference_optimum"],"source_path":str(p.relative_to(ROOT)).replace("\\","/"),"source_sha256":sha(p)})
 contract={"id":RUN,"frozen_before_formal_run":True,"design":"Eighteen previously generated small contracts. No method, seed, ranking, or parameter is changed after the controlled study. Stored references come from prior exhaustive enumeration of 4096 recipient subsets per contract.","case_count":len(cases),"random_seeds":[91001+i for i in range(16)],"methods":{"Base":"unchanged input","CRP":"byte-ordered safe deletion","FreshRatio":"D01 QoDA-inspired comparator, not QoDA reproduction","LPOrder":"frozen root-LP order plus checked deletion","ShareFirst":"D01 sharing-first lexicographic solver control, not iShare or BIGSHARED reproduction","DeliveryFirst":"delivery-first lexicographic solver control","Random16":"best and median of sixteen frozen random orders","RCRP":"one-step rollout over CRP","JCR":"exact same-domain MILP"},"cases":cases}
 assert len(cases)==18;dump(folder/"prepared"/"CONTRACT.json",contract)
 for name in ["controlled_baselines.py","contract_prune.py","joint_calendar.py","rollout_calendar.py","run_heldout_comparison.py"]:shutil.copyfile(ROOT/"src"/name,folder/"prepared"/name)
 cmd=[sys.executable,"-I","-B","-X","utf8",str(folder/"prepared"/"run_heldout_comparison.py"),"--worker"];dump(folder/"COMMAND_ENVIRONMENT_BEFORE_RUN.json",{"command":cmd,"python":sys.version,"platform":platform.platform(),"frozen":[{"path":str(p.relative_to(folder)).replace("\\","/"),"sha256":sha(p)} for p in sorted((folder/"prepared").iterdir())]})
 with (folder/"stdout.log").open("wb") as out,(folder/"stderr.log").open("wb") as err:r=subprocess.run(cmd,cwd=ROOT,stdout=out,stderr=err)
 dump(folder/"RECEIPT.json",{"exit_code":r.returncode,"state":"FINISHED_AWAITING_AUDIT" if r.returncode==0 else "FAILED_PRESERVED"});print(json.dumps({"run":RUN,"exit_code":r.returncode,"cases":len(cases)}))
 if r.returncode:print((folder/"stderr.log").read_text(errors="replace")[-6000:]);raise SystemExit(r.returncode)
if __name__=="__main__":main()
