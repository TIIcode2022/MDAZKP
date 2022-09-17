#!/usr/bin/env python
import time, sys


def all_flow(INTERFACE):
    f = open('/proc/net/dev')
    flow_info = f.readlines()
    in_flow = []
    out_flow = []
    for eth_dev in flow_info:
        if INTERFACE in eth_dev:
            in_flow.append(int(eth_dev.split(':')[1].split()[0]))
            out_flow.append(int(eth_dev.split(':')[1].split()[9]))
    f.close()
    return in_flow, out_flow


def format_flow(flow):
    flow_n = float(flow)

    return '%.3f' % (flow_n / 1024)


if __name__ == '__main__':
    fout={}
    fin={}
    if len(sys.argv) > 1:
        INTERFACE = sys.argv[1]
    else:
        INTERFACE = 'ens'

    in_flows = {}
    out_flows = {}
    all_flows0 = all_flow(INTERFACE)
    count = 0
    while True:
        time.sleep(1)

        all_flows1 = all_flow(INTERFACE)


        for x in range(len(all_flows0[0])):
            if len(sys.argv) > 1:
                curr_eth = INTERFACE
            else:
                curr_eth = 'eth%s' % x
            if len(in_flows) == x:
                in_flows[curr_eth] = []
                out_flows[curr_eth] = []
            print(
            format_flow(all_flows1[0][x] - all_flows0[0][x]) + "," + \
            format_flow(all_flows1[1][x] - all_flows0[1][x]))
            fin[time.time()]=format_flow(all_flows1[0][x] - all_flows0[0][x])
            fout[time.time()]=format_flow(all_flows1[1][x] - all_flows0[1][x])
            in_flows[curr_eth].append(all_flows1[0][x] - all_flows0[0][x])
            out_flows[curr_eth].append(all_flows1[1][x] - all_flows0[1][x])
        all_flows0 = all_flows1
        count = count + 1



    for key in in_flows:
        sum_a = 0
        in_flow = ''
        for i in in_flows[key]:
            sum_a = sum_a + i
            in_flow = in_flow + format_flow(i) + ' '
        # print key + ' flow_in is: %s'% in_flow
        print(
        key + " average of flow_in is: %s" % format_flow(sum_a / count))

    for key in out_flows:
        sum_a = 0
        out_flow = ''
        for i in out_flows[key]:
            sum_a = sum_a + i
            out_flow = out_flow + format_flow(i) + ' '
        # print key + ' flow_out is: %s'% out_flow
        print(
        key + " average of flow_out is: %s" % format_flow(sum_a / count))