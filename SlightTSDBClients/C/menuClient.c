#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h> 


static int sockfd = 0;
static char recvBuff[16000];
static struct sockaddr_in serv_addr;

int __openSocket(char *addr, uint16_t port) {

	memset(recvBuff, '0',sizeof(recvBuff));
	if((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    	printf("\n Error : Could not create socket \n");
    	return -1;
	}

	memset(&serv_addr, '0', sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(port);

    if(inet_pton(AF_INET, addr, &serv_addr.sin_addr)<=0) {
	    printf("\n inet_pton error occured\n");
    	return -1;
	}
    if( connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
		printf("\n Error : Connect Failed \n");
		return -1;
	}
    return(0);
}

int __getDPId() {
	int res;
    printf("Insert the DPId :");scanf("%d",&res);
    return(res);
}

unsigned int __getTS() {
	unsigned int res;
    printf("Insert the Timestamp (Unix Time) :");scanf("%u",&res);
    return(res);
}

int __displayMenu(void) {

	int res = 0;

    printf("---- SligthTSDB Client -v0.1--------------\n");
    printf(" 1)  print server status \n");
    printf(" 2)  get DataPoint list \n");
    printf(" 3)  get DataPoint info \n");
    printf(" 4)  create DataPoint \n");
    printf(" 5)  delete DataPoint \n");
    printf(" 6)  get last DataPoint value \n");
    printf(" 7)  get DataPoint value \n");
    printf(" 8)  set DataPoint value \n");
    printf(" 9)  get DataPoint value series \n");
    printf(" \n");
    printf(" -1)  exit \n\n");

    printf("Select function :");scanf("%d",&res);
    return(res);

}

void __getServerStatus(void) {

	int n=0;
	snprintf(recvBuff, sizeof(recvBuff), "{\"method\":\"serverstatus\"}");
    write(sockfd, recvBuff, strlen(recvBuff));
    while ( (n = read(sockfd, recvBuff, sizeof(recvBuff)-1)) > 0){
        recvBuff[n] = 0;
	}
    if(n < 0){
        printf("\n **** Read error \n");
        return;
	}
    printf(" \n Results : \n");
    printf("%s\n",recvBuff);
    return;
}


void __getDPList(void) {

	int n=0;
	snprintf(recvBuff, sizeof(recvBuff), "{\"method\":\"getdplist\"}");
    write(sockfd, recvBuff, strlen(recvBuff));
    while ( (n = read(sockfd, recvBuff, sizeof(recvBuff)-1)) > 0){
        recvBuff[n] = 0;
	}
    if(n < 0){
        printf("\n **** Read error \n");
        return;
	}
    printf(" \n Results : \n");
    printf("%s\n",recvBuff);
    return;
}

void __getDPInfo(int dpId) {

	int n=0;
	snprintf(recvBuff, sizeof(recvBuff), "{\"method\":\"getdpinfo\", \"params\" : { \"dpId\" : %d }}", dpId);
    write(sockfd, recvBuff, strlen(recvBuff));
    while ( (n = read(sockfd, recvBuff, sizeof(recvBuff)-1)) > 0){
        recvBuff[n] = 0;
	}
    if(n < 0){
        printf("\n **** Read error \n");
        return;
	}
    printf(" \n Results : \n");
    printf("%s\n",recvBuff);
    return;
}

void __deleteDP(int dpId) {

	int n=0;
	snprintf(recvBuff, sizeof(recvBuff), "{\"method\":\"deletedp\", \"params\" : { \"dpId\" : %d }}", dpId);
    write(sockfd, recvBuff, strlen(recvBuff));
    while ( (n = read(sockfd, recvBuff, sizeof(recvBuff)-1)) > 0){
        recvBuff[n] = 0;
	}
    if(n < 0){
        printf("\n **** Read error \n");
        return;
	}
    printf(" \n Results : \n");
    printf("%s\n",recvBuff);
    return;
}

void __getLastDP(int dpId) {

	int n=0;
	snprintf(recvBuff, sizeof(recvBuff), "{\"method\":\"getdplast\", \"params\" : { \"dpId\" : %d }}", dpId);
    write(sockfd, recvBuff, strlen(recvBuff));
    while ( (n = read(sockfd, recvBuff, sizeof(recvBuff)-1)) > 0){
        recvBuff[n] = 0;
	}
    if(n < 0){
        printf("\n **** Read error \n");
        return;
	}
    printf(" \n Results : \n");
    printf("%s\n",recvBuff);
    return;
}
void __getDP(int dpId, unsigned int timeStamp) {

	int n=0;
	snprintf(recvBuff, sizeof(recvBuff), "{\"method\":\"getdpval\", \"params\" : { \"dpId\" : %d , \"timestamp\": %u}}", dpId, timeStamp);
    write(sockfd, recvBuff, strlen(recvBuff));
    while ( (n = read(sockfd, recvBuff, sizeof(recvBuff)-1)) > 0){
        recvBuff[n] = 0;
	}
    if(n < 0){
        printf("\n **** Read error \n");
        return;
	}
    printf(" \n Results : \n");
    printf("%s\n",recvBuff);
    return;
}
int main(int argc, char *argv[])
{

    printf("******************************\n");
    printf("*    SlightTSDB C client     *\n");
    printf("* ver.0.1      15/01/2014    *\n");
    printf("* auth : A.Franco            *\n");
    printf("******************************\n");

    // Settings
    char IPadd[30];
    uint16_t IPport;

    printf("Insert server data ");
    printf("\n  SlightTSDB server IP port:");
    scanf("%ui", (unsigned int *)&IPport);
    printf("\n  SlightTSDB server IP address:\n");
    scanf("%99s",IPadd);

	int iChoice = 0;
    while ((iChoice = __displayMenu()) > 0) {

    	switch(iChoice) {
   		case 1:
   			if(__openSocket(IPadd, IPport)==0) __getServerStatus();
   			close(sockfd);
   			break;
   		case 2:
   			if(__openSocket(IPadd, IPport)==0) __getDPList();
   			close(sockfd);
   			break;
   		case 3:
   			if(__openSocket(IPadd, IPport)==0) __getDPInfo(__getDPId());
   			close(sockfd);
   			break;
   		case 4:
   			break;
   		case 5:
   			if(__openSocket(IPadd, IPport)==0) __deleteDP(__getDPId());
   			close(sockfd);
   			break;
   		case 6:
  			if(__openSocket(IPadd, IPport)==0) __getLastDP(__getDPId());
   			close(sockfd);
   			break;
   		case 7:
  			if(__openSocket(IPadd, IPport)==0) __getDP(__getDPId(), __getTS() );
   			close(sockfd);
   			break;
   		case 8:
    			break;
   		case 9:
    			break;
   		case -1:
   			printf("Bye bye !\n");
    		break;
   		default:
   			printf("Wrong selection :( \n\n");
   			break;
    	}
    }

}

/*
    int i, k=0;
	for(i=0;i<=500000;i++) {
		k++;	
		memset(recvBuff, '0',sizeof(recvBuff));
		if((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        	printf("\n Error : Could not create socket \n");
        	return 1;
    	} 

    	memset(&serv_addr, '0', sizeof(serv_addr)); 
    	serv_addr.sin_family = AF_INET;
    	serv_addr.sin_port = htons(8080); 

	    if(inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr)<=0) {
    	    printf("\n inet_pton error occured\n");
        	return 1;
    	} 
	    if( connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
			printf("\n Error : Connect Failed \n");
			return 1;
    	}
	    snprintf(recvBuff, sizeof(recvBuff), "{\"method\":\"setdpval\", \"params\": {\"dpId\": 2447, \"values\": [ %d , %d]} }", k, 5000-k);
        write(sockfd, recvBuff, strlen(recvBuff)); 
            if(k>5000) k=0;
  
	    while ( (n = read(sockfd, recvBuff, sizeof(recvBuff)-1)) > 0){
	        recvBuff[n] = 0;
 //   	    if(fputs(recvBuff, stdout) == EOF){
//	            printf("\n Error : Fputs error\n");
  //  	    }
    	} 
	    if(n < 0){
	        printf("\n Read error \n");
    	} 
		close(sockfd);
		usleep(50000);
		
	}	



    return 0;
}
*/
