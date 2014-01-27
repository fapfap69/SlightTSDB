!#/bin/bash
echo "{\"method\":\"serverstatus\"}" | nc localhost 8080
echo "{\"method\":\"getdplist\"}" | nc localhost 8080
echo "{\"method\":\"getdpinfo\", \"params\" : { \"dpId\" : 1945 }}" | nc localhost 8080
echo "{\"method\":\"getdplast\", \"params\": {\"dpId\": 1945 } }" | nc localhost 8080
echo "{\"method\":\"getdpval\", \"params\": {\"dpId\": 1945 , \"timestamp\": 1387034060 }}" | nc localhost 8080
echo "{\"method\":\"getdpser\", \"params\": {\"dpId\": 2448, \"metric\": 0, \"timestart\": 1387011000, \"timeend\":1387042910 } }" | nc localhost 8080
  
echo "{\"method\":\"setdpval\", \"params\": {\"dpId\": 2448, \"timestamp\": 1387090030, \"values\": [10.0, 0.0, 1.1, 2.2]} }" | nc localhost 8080
echo "{\"method\":\"setdpval\", \"params\": {\"dpId\": 2448, \"values\": [12.0, 0.0, 1.1, 2.2]} }" | nc localhost 8080

echo "{\"method\":\"createdp\", \"params\": {\"dpId\": 3000, \"metrics\": 3, \"cacheDim\": 100, \"interval\": 1, \"dbMode\":0, \"decimation\":2, \"dsModes\": [0, 0, 0]} }" | nc localhost 8080
 
