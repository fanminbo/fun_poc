#!/usr/bin/env python

import argparse
from ConfigParser import SafeConfigParser
import requests
import json
import os
import datetime
import time

class Fscluster:
    fcport="50220"
    hport="4220"
    headers = {'content-type': 'application/json'}
    verbose=0
    comp_effrt=99
    attach_retries=3
    create_retries=3
    ssh_user="localadmin"
    dpu_ids=dict()
    fs_nodeids=[]
    # List of drive uuid,  dpu_id as kwy
    drive_uuids=dict()
    # List of vols located on each drive, drive uuid is the key
    vols_on_drive=dict()
    # List of drive's dpu and slot number
    drive_location=dict()
    # assign short name to each dpu for vol placement display
    dpu_name=dict()

    def __init__(self, fcip, fcport, hport, nodeids, dpu_names):
        self.fcip=fcip
        self.fcport=fcport
        self.hport=hport
        self.fs_nodeids=nodeids
        for (nid, dname) in zip(nodeids, dpu_names):
            self.dpu_ids[nid]=[]
            dpu0_id=self.hex_string_plus(nid,8)
            self.dpu_ids[nid].append(dpu0_id)
            self.drive_uuids[dpu0_id]=[]
            self.dpu_name[dpu0_id]=dname+"-0"
            dpu1_id=self.hex_string_plus(nid,52)
            self.dpu_ids[nid].append(dpu1_id)
            self.drive_uuids[dpu1_id]=[]
            self.dpu_name[dpu1_id]=dname+"-1"
        self.set_drives_info()

    def set_fcip(self,ip):
        self.fcip=ip

    def set_verbose(self,v):
        self.verbose=v

    def set_fcport(self,port):
        self.fcport=port

    def set_fsnodeids(self,nodeids):
        self.fs_nodeids=nodeids

    def hex_string_plus(self, hexstr, addition):
        orig="".join(hexstr.split(':'))
        newdec=int(orig,16)+addition
        newhex='{0:02x}'.format(newdec)
        res=list(newhex)
        for i in [10,8,6,4,2]:
            res.insert(i,":")
        return "".join(res)

    def log(self,s,url={},params={}):
        if self.verbose:
        #ts=datetime.datetime.now().strftime("%d.%b %Y %H:%M:%S")
            ts=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            print ts+': '+s+'URL='+str(url)+' params='+str(params)

    def send_request(self,type,url,params={}):
        rr={}
        self.log('Request=',url,params)
        ts1=datetime.datetime.now()
        if type is 'put':
            rr=requests.put(url,auth=('admin', 'password'),headers=self.headers,json=params)
        elif type is 'get':
            rr=requests.get(url,auth=('admin', 'password'),headers=self.headers,json=params)
        elif type is 'post':
            rr=requests.post(url,auth=('admin', 'password'),headers=self.headers,json=params)
        elif type is 'patch':
            rr=requests.patch(url,auth=('admin', 'password'),headers=self.headers,json=params)
        elif type is 'delete':
            rr=requests.delete(url,auth=('admin', 'password'),headers=self.headers,json=params)
        else:
            return

        ts2=datetime.datetime.now()
        tt=ts2-ts1
        self.log('Response=',params=rr.json())
        if self.verbose:
            print('Time taken='+str(tt.microseconds)+' usec')
        return rr

    def set_drives_info(self):
        ret = []
        params={}
        url = 'http://'+self.fcip+':'+self.fcport+'/FunCC/v1/topology'
        rr=self.send_request('get',url,params)
        # Reset self.drive_uuids, so this function can run multiple time
        for fs_nodeid in self.fs_nodeids:
            for dpu in self.dpu_ids[fs_nodeid]:
                self.drive_uuids[dpu]=[" " for i in range(12)]

        for fs_nodeid in self.fs_nodeids:
            for i in range(2):
                for j in range(12):
		    try:
                        dr=rr.json()['data'][fs_nodeid]['dpus'][i]['drives'][j]['uuid']
		    except:
		        dr=''
                        
                    dpu=rr.json()['data'][fs_nodeid]['dpus'][i]['drives'][j]['dpu']
                    slot=rr.json()['data'][fs_nodeid]['dpus'][i]['drives'][j]['slot_id']
                    loca=dpu+","+str(slot)
                    self.drive_uuids[dpu][slot]=dr
                    self.drive_location[dr]=loca
        return ret

    def get_drives(self):
        if self.verbose:
            for node in self.fs_nodeids:
                for dpu in self.dpu_ids[node]:
                    print "====== DPU " + dpu + " =========="
                    for drive in self.drive_uuids[dpu]:
                        slot=self.drive_location[drive].split(',')[1]
                        print drive + ":  " + slot
        return self.drive_location.keys()

    def format_drives(self,drives):
        for d in drives:
            print 'Formatting drive: ',d
            url = 'http://'+self.fcip+':'+self.fcport+'/FunCC/v1/topology/drives/'+d
            rr=self.send_request('put',url)

    def get_storage_pool(self):
        url = 'http://'+self.fcip+':'+self.fcport+'/FunCC/v1/storage/pools'
        rr=self.send_request('get',url)
        return(rr.json()['data'].keys())

    def create_raw_volume(self,pool,vname,size,encryption_key='',expand=False,stripe=0,protect={}):
        url = 'http://'+self.fcip+':'+self.fcport+'/FunCC/v1/storage/volumes'
        params={}
        params['name']=vname
        params['capacity']=size
        params['vol_type']='VOL_TYPE_BLK_LOCAL_THIN'
        if encryption_key:
            params['encrypt']=True
            params['kmip_secret_key']=encryption_key
        params['allow_expansion']=expand
        params['stripe_count']=stripe
        params['data_protection']=protect
        for i in range(self.create_retries):
            print 'Creating RAW volume: ', vname
            rr=self.send_request('post',url,params)
            if rr.json()['status']:
                break
            else:
                time.sleep(0.05)
                print 'Vol create failed, attempt: ',i,'Retrying'
        return(rr.json()['data']['uuid'])

    def create_durable_volume(self,pool,vname,size,compress=0,encryption_key='',expand=False):
        url = 'http://'+self.fcip+':'+self.fcport+'/FunCC/v1/storage/volumes'
        params={}
        params['name']=vname
        params['capacity']=size
        params['vol_type']='VOL_TYPE_BLK_EC'
        params['compression_effort']=compress
        if encryption_key:
            params['encrypt']=True
            params['kmip_secret_key']=encryption_key
        params['allow_expansion']=expand
        protect = {}
        protect['num_failed_disks'] = 2
        protect['num_redundant_dpus'] = 1
        params['data_protection'] = protect
        print 'Creating Durable volume: ',vname
        rr=self.send_request('post',url,params)
        return(rr.json()['data']['uuid'])

    def get_hostnqn(self,host_ip):
        host_nqn='nqn.2015-09.com.fungible:'+host_ip
        return host_nqn

    def attach_vol_to_host(self,vol_uuid,server):
        url = 'http://'+self.fcip+':'+self.fcport+'/FunCC/v1/storage/volumes/'+str(vol_uuid)+'/ports'
        params = {}
        host_nqn=self.get_hostnqn(server)
        params['host_nqn']=host_nqn
        params['transport']='TCP'
        print 'Attaching volume:'
        for i in range(self.attach_retries):
            rr=self.send_request('post',url,params)
            if rr.json()['status']:
                break
            else:
                time.sleep(0.05)
                print 'Attach failed, attempt: ',i,'Retrying'
        #return(rr.json()['data']['uuid'])
        return(rr.json())

    def get_ports_vols(self,mode='print'):
        ports=[]
        vols=[]
        url = 'http://'+self.fcip+':'+self.fcport+'/FunCC/v1/storage/volumes'
        params = {}
        rr=self.send_request('get',url,params)
        if mode == 'print':
            print 'Port List:'
            print rr.json()
    
        for i in rr.json()['data'].keys():
            try:
                for pp in rr.json()['data'][i]['ports'].keys():
                    ports.append(pp)
            except:
                ports.append('')
            vols.append(rr.json()['data'][i]['uuid'])

        return ports,vols 

    def get_volume_details(self,vol,field='all',mode='print'):
        url = 'http://'+self.fcip+':'+self.fcport+'/FunCC/v1/storage/volumes/'+vol
        params = {}
        rr=self.send_request('get',url,params)
        if mode == 'print':
            print 'Volume details for: '+vol,
            if field == 'all':
                print rr.json()
            else:
                print rr.json()['data'][field]

        return rr

    def print_placement_details(self):
        print "==== DPU id (MAC) and Name mapping ====="
        for nid in self.fs_nodeids:
            for did in self.dpu_ids[nid]:
                print did + " : " + self.dpu_name[did] 
        #print "========================================"

        raw_vols_name=[]
        dur_vols_name=[]
        vol_name_to_id=dict()
        vols_on_drive=dict()
        vol_to_drives=dict()

        for node in self.fs_nodeids:
            for dpu in self.dpu_ids[node]:
                for drive in self.drive_uuids[dpu]:
                    vols_on_drive[drive]=[]

        url = 'http://'+self.fcip+':'+self.fcport+'/FunCC/v1/storage/volumes'
        params = {}
        rr=self.send_request('get',url,params)
        for i in rr.json()['data'].keys():
            v_type=rr.json()['data'][i]['type']
            uuid=rr.json()['data'][i]['uuid']
            name=rr.json()['data'][i]['name']
            vol_name_to_id[name]=uuid
            vol_to_drives[name]=[]
            dd=self.get_volume_details(uuid,mode='silent')
            if v_type == "raw volume":
                raw_vols_name.append(name)
                duuid=dd.json()['data']['drive_uuid']
                vols_on_drive[duuid].append(name)
                #vols_on_drive[duuid].sort()
                #index=vols_on_drive[duuid].index(name)
                vol_to_drives[name].append(duuid)
            if v_type == "durable volume":
                dur_vols_name.append(name)
                for vol in dd.json()['data']['src_vols']:
                    ee=self.get_volume_details(vol,mode='silent')
                    type=ee.json()['data']['type']
                    if type == "VOL_TYPE_BLK_EC":
                        for vol in ee.json()['data']['src_vols']:
                            ff=self.get_volume_details(vol,mode='silent')
                            duuid=ff.json()['data']['drive_uuid']
                            vols_on_drive[duuid].append(name)
                            #vols_on_drive[duuid].sort()
                            #index=vols_on_drive[duuid].index(name)
                            vol_to_drives[name].append(duuid)

        print ">>>>>>>>>>>>> Disk to Volumes Mapping <<<<<<<<<<<<<<<<<" 
        for node in self.fs_nodeids:
            for dpu in self.dpu_ids[node]:
                print "==== DPU: "+self.dpu_name[dpu]+" "+dpu+" ===="  
                for drive in self.drive_uuids[dpu]:
                    if len(vols_on_drive[drive]) > 0:
                        slot=self.drive_location[drive].split(',')[1]
                        print drive+" "+slot+" "+ ' '.join(sorted(vols_on_drive[drive]))

        print ">>>>>>>>>>>>> Volume to Disks Mapping <<<<<<<<<<<<<<<<<" 
        print "======== Raw Volumes ========"
        for vname in sorted(raw_vols_name):
            ss=""
            for did in vol_to_drives[vname]:
                dpu=self.drive_location[did].split(',')[0]
                slot=self.drive_location[did].split(',')[1]
                ss=ss+self.dpu_name[dpu]+"/"+slot+" " 
            print vname+": "+self.dpu_name[dpu]+"/"+slot
        print "======== Dura Volumes ========"
        for vname in sorted(dur_vols_name):
            lst=[]
            for did in vol_to_drives[vname]:
                dpu=self.drive_location[did].split(',')[0]
                slot=self.drive_location[did].split(',')[1]
                lst.append(self.dpu_name[dpu]+"/"+slot)
            ss=' '.join(sorted(lst))
            print vname+": "+ss

    def detach_volumes(self,ports):
        for i in ports:
            url = 'http://'+self.fcip+':'+self.fcport+'/FunCC/v1/storage/ports/'+i
            params = {}
            # Volumes might not be attached
            try:
                print 'Detaching Volumei:',i
                for i in range(self.attach_retries):
                    rr=self.send_request('delete',url,params)
                    if rr.json()['status']:
                        break
                    else:
		        time.sleep(0.05)
                        print 'Detach failed, attempt: ',i,'Retrying'

                print rr.json()
                #time.sleep(1)
            except:
                pass

    def delete_single_volume(self,vol):
        url = 'http://'+self.fcip+':'+self.fcport+'/FunCC/v1/storage/volumes/'+vol
        params = {}
        print 'Deleting Volume:',vol
        for i in range(self.attach_retries):
            rr=self.send_request('delete',url,params)
            if rr.json()['status']:
                break
            else:
                time.sleep(0.05)
                print 'Delete failed, attempt: ',i,'Retrying'
        rr=self.send_request('delete',url,params)
        print rr.json()

    def delete_volumes(self,vols):
        for i in vols:
            #url = 'http://'+fcip+':'+fcport+'/FunCC/v1/storage/volumes/'+i
            #params = {}
            #print 'Deleting Volume:',i
            #rr=send_request('delete',url,params)
            #print rr.json()
            self.delete_single_volume(i)

    def get_vol_details(self,vol):
        url = 'http://'+self.fcip+':'+self.fcport+'/FunCC/v1/storage/volumes/'+str(vol)
        params = {}
        rr=self.send_request('get',url,params)
        return rr.json()

    def print_nvme_attach_cmd(self,volid,pid,server):
        vd=self.get_vol_details(volid)
        svd=vd['data']['ports'][pid]
        transport=svd['transport'].lower()
        dpuip=svd['ip']
        host_nqn=svd['host_nqn']
        subsys_nqn=svd['subsys_nqn']
        print 'ssh '+self.ssh_user+'@'+server+' sudo nvme connect -t '+transport+' -a '+dpuip+' -s '+self.hport+' -n '+subsys_nqn+' -q '+host_nqn

    def get_vol_dpu(self,vol):
        ret=self.get_vol_details(vol)
        dpu_mac=ret['data']['dpu']
        return dpu_mac

    def get_vol_name(self,vol):
        ret=self.get_vol_details(vol)
        vol_name=ret['data']['name']
        return vol_name

