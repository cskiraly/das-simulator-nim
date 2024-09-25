# we parse the latencies(x) file produced by run.sh to receive results summary (Max/Avg Latency --> per packet, overall)
# runs $awk -f result_summary.awk latencies(x)

BEGIN {
	FS = " ";		#default column separator
	network_size = 0
	max_nw_lat = sum_nw_lat = 0
	hop_lat = 1000	#should be consistent with shadow.yaml
}

{
	clean_int = $3
	gsub(/[^0-9]/, "", clean_int); 
	if ($3 == clean_int){	#get rid of unwanted rows
		sum_nw_lat += $NF 
		if (max_nw_lat < $NF) {max_nw_lat = $NF} 
		if (split($1, arr, "peer|/main|:.*:")) {
				#$3 = rx_latency,	arr[4] = publish_time,	arr[2] = peerID
				lat_arr[arr[4], $3]++;	
				msg_arr[arr[4]] = 1;	#we maintain set of messages identified by their publish time
				if (network_size < arr[2]) {network_size = arr[2]}
		}
	}
}

END {

	print "Total Nodes : ", network_size, "Total Messages Published : ", length(msg_arr), 
            "Network Latency\t MAX : ", max_nw_lat, "\tAverage : ", sum_nw_lat/NR
	print "ID\t Avg Latency \t Messages Received"
	for (value in msg_arr) {
		sum_rx_msgs = 0;
        latency = 0;
		for (key in lat_arr) {
			split(key, parts, SUBSEP);
			if (parts[1] == value) {
				sum_rx_msgs = sum_rx_msgs + lat_arr[key]; 					#total receives / message
				latency = latency + (lat_arr[key] * parts[2]) 
				spread[ int((parts[2]) / hop_lat) ] += lat_arr[key]           #hop-by-hop spread count of messages
				#print int((parts[2]) / hop_lat), lat_arr[key]
	    	}
	    }

		print value, "\t", latency/sum_rx_msgs, "\t  ", sum_rx_msgs, "with spread:", 
                spread[0], spread[1], spread[2], spread[3], spread[4], spread[5], spread[6], spread[7],   
                spread[8], spread[9], spread[10], spread[11], spread[12], spread[13], spread[14], spread[15]   
        delete spread
    }
}


