#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/select.h>
#include <arpa/inet.h>

#define MAX_SERVERS  16 //Maybe
#define MAX_LINE     512
#define MAX_NAME     256
#define MAX_PAIR_STR 8
#define MAX_BUF      8192

typedef struct {
    char name[MAX_NAME];
    char ip[64];
    int  port;
} server;

static server servers[MAX_SERVERS];
static int       num_servers = 0;

// the four canonical pairs
static const int base_pairs[4][2] = {
    {1,2},
    {2,3},
    {3,4},
    {4,1}
};

// forward declarations
int  parse_config(void);
void do_list(void);
void do_put(const char *filename);
void do_get(const char *filename);
void printServers();
int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr,"Usage: %s <list|get|put> [filenames...]\n", argv[0]);
        return 1;
    }
    if (parse_config() < 0) return 1;

    if (strcmp(argv[1],"list")==0) {
        do_list();
    }
    else if (strcmp(argv[1],"put")==0) {
        for (int i = 2; i < argc; i++)
            do_put(argv[i]);
    }
    else if (strcmp(argv[1],"get")==0) {
        for (int i = 2; i < argc; i++)
            do_get(argv[i]);
    }
    else {
        fprintf(stderr,"Unknown cmd '%s'\n", argv[1]);
        return 1;
    }
    return 0;
}
void printServers(){
    for (int i = 0; i < num_servers; i++){
        server currServer = servers[i];
        printf("*********\n"
               "Server %d\n"
               "Name: %s\n"
               "Ip: %s\n"
               "Portno: %d\n",
                i, currServer.name, currServer.ip, currServer.port);
    }
}
// ----------------------------------------------------------------------------
// load ~/dfc.conf lines like: server dfs1 127.0.0.1:10001
int parse_config(void) {
    FILE* configPtr = fopen("./dfc.conf", "r");
    if (!configPtr) {perror("Couldnt open ./dfc.conf"); return -1;}

    char line[MAX_LINE];
    char entryType[MAX_NAME];
    char serverName[MAX_NAME];
    char addressWithPort[MAX_NAME];
    char* port;
    int portno;

    while (fgets(line, MAX_LINE, configPtr)){
        if (sscanf(line, "%s %s %s", entryType, serverName, addressWithPort) != 3)  {fprintf(stderr,"Invalid Entry in Config File!\n"); return -1;};
        port = strchr(addressWithPort, ':')+1;
        portno = atoi(port);
        *(port-1) = '\0';

        strcpy(servers[num_servers].ip, addressWithPort);
        strcpy(servers[num_servers].name, serverName);
        servers[num_servers].port = portno;
        num_servers+=1;
    }
}
//Initiate a TCP connection and return a file descriptor
int do_tcp(char* ipString, int port){
    int sockfd;
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {perror("Error in socket creation"); return -1;}

    struct timeval timeout;      
    timeout.tv_sec = 0;
    timeout.tv_usec = 500;
    if (setsockopt (sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout,sizeof timeout) < 0 || setsockopt (sockfd, SOL_SOCKET, SO_SNDTIMEO, &timeout,sizeof timeout) < 0){
        close(sockfd);
        perror("setsockopt failed ");
        return -1;
    }

    struct sockaddr_in serverAddr;
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    if (inet_pton(AF_INET, ipString, &serverAddr.sin_addr) <= 0){ perror("Invalid Ip String"); close(sockfd); return -1;}
    serverAddr.sin_port = htons(port);
    
    if (connect(sockfd, (struct sockaddr *) &serverAddr, sizeof(serverAddr)) < 0) { perror("Error in connect"); close(sockfd); return -1;}

    return sockfd;
}
void do_list(void) {
    char recvBuffer[MAX_BUF];
    int n;
    for (int i = 0; i < num_servers; i++){
        server currServer = servers[i];
        int sockHandle = do_tcp(currServer.ip,currServer.port);
        if (send(sockHandle, "list", strlen("list"), 0) < 0) {perror("Error in send('list')"); close(sockHandle); return;}

        while (n = recv(sockHandle, recvBuffer, MAX_BUF, 0)){
            if (n <= 0){break;}
            if (strstr(recvBuffer, "END")) {break;}
            fwrite(recvBuffer, 1, n, stdout);
        }
        close(sockHandle);
    }
}

// ----------------------------------------------------------------------------
// put: split file into 4, send each server its overlapping pair
void do_put(const char *filename) {
    
}

// ----------------------------------------------------------------------------
// get: fetch each pair, rebuild chunks 1–4, write file
void do_get(const char *filename) {
    
}