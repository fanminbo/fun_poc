#!/usr/bin/env python

import argparse
from ConfigParser import SafeConfigParser
import requests
import json
import os
import datetime
import time
from  Fscluster import *

comp_effrt=99

def get_server(n):
    return dict(zip(server_list.keys()[n].split(),server_list.values()[n].split()))

#
# MAIN
#
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config-file', required=False, help='Config file', \
        default='mysysinfo.in')
    parser.add_argument('-s', '--show-drives', action='store_true', required=False, \
        help='Show drives only',default=False)
    parser.add_argument('-l', '--list-volumes', action='store_true', required=False, \
        help='List Volumes only',default=False)
    parser.add_argument('-lf', '--list-volume-field', required=False, \
        help='List Volumes only',default=None)
    parser.add_argument('-f', '--format-drives', action='store_true', required=False, \
        help='Format drives only',default=False)
    parser.add_argument('-d', '--delete-volumes', action='store_true', required=False, \
        help='Delete volumes only',default=False)
    parser.add_argument('-v', '--verbose', action='store_true', required=False, \
        help='Verbose output',default=0)
    parser.add_argument('-b', '--background-create-delete', required=False, \
        help='Create Delete Volumes- needs 2 values - start & end volnum', nargs='*')
    parser.add_argument('-na', '--no-attach', action='store_true', required=False, \
        help='Only create volumes, do not attach',default=False)
    parser.add_argument('-p', '--placement-details', action='store_true', required=False, \
        help='Print Volume Placement details',default=False)

    args, unknown = parser.parse_known_args()
    if args.verbose:
        verbose = 1
    else:
        verbose = 0

    if not os.path.isfile(args.config_file):
        print 'Error: File not found:', args.config_file
        exit(1)

    parser = SafeConfigParser()
    parser.read(args.config_file)

    setupinfo = {}
    sip=''.join(parser.sections())
    setupinfo[sip] = {}
    for option in parser.options(sip):
	setupinfo[sip][option] = parser.get(sip,option)

    si = setupinfo[sip]
    fcip = si['fcip']
    fcport = si['fcport']
    hport = si['hport']
    fs_nodeids = si['fs_nodeid'].split()
    fs_dpu_names = si['fs_dpu_name'].split()
    comp_effrt = int(si['compression_effort'])
    encr_key = si['encryption_key']
    srvrs = si['servers']
    srvrips = si['server_ips']
    server_list = dict(zip(srvrs.split(), srvrips.split()))
    username = si['username']
    nvols = int(si['nvols'])
    raw_volsize =int(si['raw_volsize'])
    rawenc_volsize = int(si['rawenc_volsize'])
    dur_volsize =int(si['dur_volsize'])
    durcomp_volsize =int(si['durcomp_volsize'])
    durenc_volsize =int(si['durenc_volsize'])
    durcompenc_volsize =int(si['durcompenc_volsize'])
    start_vol_num = int(si['start_vol_num'])
    voltypes = si['voltypes'].split(' ')
    attach_mode = si['attach_mode']
    create_retries = int(si['create_retries'])
    attach_retries = int(si['attach_retries'])

    fs1600=Fscluster(fcip,fcport,hport,fs_nodeids,fs_dpu_names)
    fs1600.set_sshuser(username)
    fs1600.set_verbose(verbose)

    if args.show_drives:
        drives=fs1600.get_drives()
        print drives
        exit(0)

    if args.format_drives:
        drives=fs1600.get_drives()
        fs1600.format_drives(drives)
        exit(0)

    if args.delete_volumes:
        p,v=fs1600.get_ports_vols()
        fs1600.detach_volumes(p)
        fs1600.delete_volumes(v)
        exit(0)

    if args.list_volumes:
        p,v=fs1600.get_ports_vols()
        print 'Ports:',p
        print 'Volumes:',v
        for vol in v:
            fs1600.get_volume_details(vol)
        exit(0)

    if args.list_volume_field:
        p,v=fs1600.get_ports_vols()
        for vol in v:
            fs1600.get_volume_details(vol,args.list_volume_field)
        exit(0)

    if args.placement_details:
        fs1600.print_placement_details()
        exit(0)

    if args.background_create_delete:
	svol=int(args.background_create_delete[0])
	evol=int(args.background_create_delete[1])
        v='dur'
        spool=get_storage_pool()
        vols={}
        tss=datetime.datetime.now()
        for i in range(svol,evol):
            volname='{0}{1:04d}'.format(v,i)
            vols[i]=create_durable_volume(spool,volname,dur_volsize)
        tse=datetime.datetime.now()
        tt=tse-tss
        print('Total Volume Create Time = '+str(tt)+' usec')
        tss=datetime.datetime.now()
        for i in range(svol,evol):
            delete_single_volume(vols[i])
        tse=datetime.datetime.now()
        tt=tse-tss
        print('Total Volume Delete Time = '+str(tt)+' usec')
        exit(0)

    spool=fs1600.get_storage_pool()
    print "spool is: ", spool
    tss=datetime.datetime.now()
    num_dpu=len(fs_nodeids)*2
    for v in voltypes:
        i=start_vol_num
        attached_dpu=dict()
        host_list=srvrips.split()
        num_host=len(host_list)
        attached_vols=0
        vol_per_host=(nvols+num_host-1)//num_host

        for server in server_list:
            sip=server_list[server]
            attached_dpu[sip]=[]

        while attached_vols < nvols:
            volname='{0}{1:04d}'.format(v,i)
            attach_done=0
            host_index=0

            if v == 'raw':
                volid=fs1600.create_raw_volume(spool,volname,raw_volsize)
            elif v == 'rawenc':
                volid=fs1600.create_raw_volume(spool,volname,rawenc_volsize,encryption_key=encr_key)
            elif v == 'dur':
                volid=fs1600.create_durable_volume(spool,volname,dur_volsize)
            elif v == 'durcomp':
                volid=fs1600.create_durable_volume(spool,volname,durcomp_volsize,compress=comp_effrt)
            elif v == 'durenc':
                volid=fs1600.create_durable_volume(spool,volname,durenc_volsize,encryption_key=encr_key)
            elif v == 'durcompenc':
                volid=fs1600.create_durable_volume(spool,volname,durcompenc_volsize,compress=comp_effrt, \
                    encryption_key=encr_key)
            else:
                print 'Error: Unsupported volume type:',v

            print 'Volume',v,'created, UUID=',volid

            if not args.no_attach:
                if attach_mode == 'cyclic':
                    servers=get_server(i%len(server_list))
                else:
                    servers=server_list

                vol_dpu=fs1600.get_vol_dpu(volid)
                # attach_done = 0 and i = 0
                while attach_done == 0 and host_index < len(host_list):
                    host=host_list[host_index]
                    if vol_dpu not in attached_dpu[host]:
                        rr=fs1600.attach_vol_to_host(volid,host)
                        pid=rr['data']['uuid']
                        fs1600.print_nvme_attach_cmd(volid,pid,host)
                        attached_dpu[host].append(vol_dpu)
                        attach_done=1
                        attached_vols+=1
                        nv=len(attached_dpu[host])
                        if nv == num_dpu or nv == vol_per_host:
                            attached_dpu[host]=[]
                            host_list.pop(host_index)
                        else:
                            host_list.append(host_list.pop(host_index))
                    else:
                        host_index+=1
                
                if len(host_list) == 0:
                    host_list=srvrips.split()
                    vol_per_host=vol_per_host-num_dpu
            i+=1

    tse=datetime.datetime.now()
    tt=tse-tss
    if verbose:
        ttt=(tt.microseconds + 0.0 + (tt.seconds + tt.days * 24 * 3600) * 10 ** 6) / 10 ** 6
        #print('Total Volume Time taken='+str(tt.total_seconds())+' usec')
        print('Total Volume Time taken ttt = '+str(ttt))
        print('Total Volume Time taken timedelta = '+str(tt)+' usec')
