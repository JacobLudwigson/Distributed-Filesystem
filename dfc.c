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
#include <unistd.h>
#include <sys/select.h>
#include <arpa/inet.h>
#include <math.h>
#include <openssl/md5.h>

#define MAX_SERVERS  16 
#define MAX_FILES_PER_SERVER 32
#define MAX_LINE     512
#define MAX_NAME     256
#define MAX_PAIR_STR 8
#define MAX_BUF      8192

typedef struct {
    char name[MAX_NAME];
    char ip[64];
    int  port;
    int active;
} server;

static server servers[MAX_SERVERS];
static int       num_servers = 0;

// forward declarations
int  parse_config(void);
void do_list(int verbose);
void do_put(const char *filename, int numActiveServers);
void do_get(const char *filename);
void printServers();
int ping_active();

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr,"Usage: %s <list|get|put> [filenames...]\n", argv[0]);
        return 1;
    }
    if (parse_config() < 0) return 1;
    int numActiveServers = ping_active();
    if (numActiveServers == 0){fprintf(stderr, "No active servers!\n"); return 0;} 

    if (strcmp(argv[1],"list")==0) {
        do_list(1);
    }
    else if (strcmp(argv[1],"put")==0) {
        for (int i = 2; i < argc; i++)
            do_put(argv[i], numActiveServers);
    }
    else if (strcmp(argv[1],"get")==0) {
        for (int i = 2; i < argc; i++)
            do_get(argv[i]);
    }
    else {
        fprintf(stderr,"Unknown cmd '%s'\n", argv[1]);
        return 1;
    }
    printServers();
    return 0;
}
void printServers(){
    for (int i = 0; i < num_servers; i++){
        server currServer = servers[i];
        printf("*********\n"
               "Server %d\n"
               "Name: %s\n"
               "Ip: %s\n"
               "Portno: %d\n"
               "Active: %d\n",
                i, currServer.name, currServer.ip, currServer.port, currServer.active);
        if (i == num_servers-1){printf("*********\n");}
    }
}
long long compute_md5(const char* filename, unsigned char* hashBuffer){
    unsigned char md5Hash[MD5_DIGEST_LENGTH];
    MD5_CTX ctx;
    MD5_Init(&ctx);
    MD5_Update(&ctx, filename, strlen(filename));
    MD5_Final(md5Hash, &ctx);
    for (int i = 0; i < MD5_DIGEST_LENGTH; i++) {
        sprintf((char*)(hashBuffer + i * 2), "%02x", md5Hash[i]);
    }
    unsigned long long a = *(unsigned long long*)hashBuffer;
    unsigned long long b = *(unsigned long long*)(hashBuffer + 8);
    return a ^ b;
}
void safeFilename(char *hex_str) {
    for (int i = 0; i < MD5_DIGEST_LENGTH*2; i++) {
        if (hex_str[i] == '/') {
            hex_str[i] = '_';
        }
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
        if (sscanf(line, "%s %s %s", entryType, serverName, addressWithPort) != 3)  {fprintf(stderr,"Invalid Entry in Config File!\n"); return -1;}
        port = strchr(addressWithPort, ':')+1;
        portno = atoi(port);
        *(port-1) = '\0';

        strcpy(servers[num_servers].ip, addressWithPort);
        strcpy(servers[num_servers].name, serverName);
        servers[num_servers].port = portno;
        servers[num_servers].active = 1;
        num_servers+=1;
    }
    return 0;
}

//Initiate a TCP connection and return a file descriptor
int do_tcp(char* ipString, int port){
    int sockfd;
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {perror("Error in socket creation"); return -1;}

    struct timeval timeout;      
    timeout.tv_sec = 2;
    timeout.tv_usec = 0;
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
void do_list(int verbose) {
    char recvBuffer[MAX_BUF];
    int n = 1;
    for (int i = 0; i < num_servers; i++){
        server currServer = servers[i];
        if (verbose) printf("*****%s*****\n",currServer.name);
        if (!currServer.active){continue;}
        int sockHandle = do_tcp(currServer.ip,currServer.port);
        if (sockHandle < 0){servers[i].active = 0; continue;}
        if (send(sockHandle, "list", strlen("list"), 0) < 0) {perror("Error in send('list')"); servers[i].active = 0; close(sockHandle); continue;}
        while (n > 0){
            n = recv(sockHandle, recvBuffer, MAX_BUF-1, 0);
            if (n < 0){
                servers[i].active = 0;
            }
            if (n == 0){break;}
            recvBuffer[MAX_BUF-1] = '\0';
            if (strstr(recvBuffer, "END")) {break;}
            if (verbose) {fwrite(recvBuffer, 1, n+1, stdout);}
        }
        close(sockHandle);
    }
}
int ping_active(){
    int activeServers = 0;
    do_list(0);
    for (int i = 0; i < num_servers; i++){
        activeServers += servers[i].active;
    }
    return activeServers;
}
// ----------------------------------------------------------------------------
// put: split file into 4, send each server its overlapping pair
void do_put(const char *filename, int numActiveServers) {
    FILE* fptr = fopen(filename, "r");
    fseek(fptr, 0, SEEK_END);
    long int totalFileSize = ftell(fptr);
    rewind(fptr);
    unsigned char digest[2*MD5_DIGEST_LENGTH+1];
    
    int startIndex = compute_md5(filename,digest) % numActiveServers;
    printf("The Start is %d\n", startIndex);

    char name[MAX_LINE];
    char chunkPair[MAX_PAIR_STR];

    int base_chunk_size = totalFileSize / numActiveServers;
    int last_chunk_extra = totalFileSize % numActiveServers;

    char **chunks = malloc(numActiveServers * sizeof(char *));
    int *chunk_sizes = malloc(numActiveServers * sizeof(int));
    for (int i = 0; i < numActiveServers; i++) {
        int size = base_chunk_size + (i == numActiveServers - 1 ? last_chunk_extra : 0);
        chunks[i] = malloc(size);
        fread(chunks[i], 1, size, fptr);
        chunk_sizes[i] = size;
    }
    fclose(fptr);
    int upperChunkPairNum = (startIndex+1) % numActiveServers;
    int lowerChunkPairNum = startIndex;
    int bytes_to_send;
    for (int i = 0; i < num_servers; i++){

        bytes_to_send = chunk_sizes[lowerChunkPairNum] + chunk_sizes[upperChunkPairNum] + 8;

        server currServer = servers[i];
        snprintf(chunkPair, sizeof(chunkPair), "%d,%d", lowerChunkPairNum+1, upperChunkPairNum+1);

        if (!currServer.active){continue;}
        int sockHandle = do_tcp(currServer.ip,currServer.port);

        //Format: command [filename] [chunkPair] [length]\n
        snprintf(name,sizeof(name), "put [%d].%s %s %d\n",numActiveServers, filename, chunkPair, bytes_to_send);
        char ack[4];
        int n;
        if (send(sockHandle, name, strlen(name), 0) < 0) {perror("Error in send('Put Headers')"); servers[i].active = 0; close(sockHandle); i = 0; continue;}
        recv(sockHandle, ack, 1, 0);

        uint32_t sz1 = htonl((uint32_t)chunk_sizes[lowerChunkPairNum]);
        uint32_t sz2 = htonl((uint32_t)chunk_sizes[upperChunkPairNum]);

        n = send(sockHandle, &sz1, 4, 0);
        n += send(sockHandle, &sz2, 4, 0);


        ssize_t sent = 0;
        while (sent < chunk_sizes[lowerChunkPairNum]) {
            ssize_t w = send(sockHandle,
                            chunks[lowerChunkPairNum] + sent,
                            chunk_sizes[lowerChunkPairNum] - sent,
                            0);
            if (w <= 0) { perror("Error sending chunk1"); break; }
            sent += w;
        }

        sent = 0;
        while (sent < chunk_sizes[upperChunkPairNum]) {
            ssize_t w = send(sockHandle,
                            chunks[upperChunkPairNum] + sent,
                            chunk_sizes[upperChunkPairNum] - sent,
                            0);
            if (w <= 0) { perror("Error sending chunk2"); break; }
            sent += w;
        }
        shutdown(sockHandle, SHUT_WR);
        
        lowerChunkPairNum = upperChunkPairNum;
        upperChunkPairNum = (upperChunkPairNum + 1) % numActiveServers;
        close(sockHandle);
    }
    for (int i = 0; i < numActiveServers; i++) {
        free(chunks[i]);
    }
    free(chunk_sizes);
    free(chunks);
}

// ----------------------------------------------------------------------------
// get: fetch each pair, rebuild chunks 1–4, write file
void do_get(const char *filename) {
    char name[MAX_NAME];
    char serverFilename[MAX_NAME];
    char fileMetaData[8];
    snprintf(name,sizeof(name), "get %s\n",filename);
    char** chunks = NULL;
    int* chunk_sizes = NULL;
    int serversActiveWhilePut = 0;
    for (int i = 0; i < num_servers; i++){
        server currServer = servers[i];

        if (!currServer.active){continue;}
        int sockHandle = do_tcp(currServer.ip,currServer.port);

        if (send(sockHandle, name, strlen(name), 0) < 0) {perror("Error in send('get header')"); servers[i].active = 0; close(sockHandle); continue;}
        int n = recv(sockHandle, serverFilename, MAX_NAME, 0);
        if (n <= 0){perror("Error in receive filename"); close(sockHandle); servers[i].active = 0; close(sockHandle); continue;}
        if (strcmp(serverFilename, "ERROR\n") == 0){printf("Server side error on filename verification");continue;}

        if (send(sockHandle, "\n", 1, 0) < 0) {perror("Error in send('Get Ack 1')"); servers[i].active = 0; close(sockHandle); continue;}
        n = recv(sockHandle, fileMetaData, 8, 0);
        if (n <= 0){perror("Error in receive metadata"); close(sockHandle); servers[i].active = 0; close(sockHandle); continue;}

        uint32_t net_len1, net_len2;
        memcpy(&net_len1, fileMetaData,     4);
        memcpy(&net_len2, fileMetaData + 4, 4);
        int32_t firstChunkLen  = ntohl(net_len1);
        int32_t secondChunkLen = ntohl(net_len2);
        if (send(sockHandle, "\n", 1, 0) < 0) {perror("Error in send('Get Ack 2')"); servers[i].active = 0; close(sockHandle); continue;}

        char* passDir = strchr(serverFilename, '/')+1;
        sscanf(passDir, "[%d].%*s", &serversActiveWhilePut);
        char* chunkPair = strrchr(serverFilename, '.')+1;
        int firstChunkNum, secondChunkNum;
        sscanf(chunkPair, "%d,%d", &firstChunkNum, &secondChunkNum);
        firstChunkNum-=1;
        secondChunkNum-=1;
        printf("Here is the filename %s\n", serverFilename);
        if (!chunks){
            chunks = malloc(serversActiveWhilePut * sizeof(char *));
            chunk_sizes = malloc(serversActiveWhilePut * sizeof(int));
        }

        if (!chunks[firstChunkNum]){
            chunks[firstChunkNum] = malloc(sizeof(char) * firstChunkLen);
        }
        chunk_sizes[firstChunkNum] = firstChunkLen;
        int remaining = firstChunkLen;
        while (remaining > 0) {
            n = recv(sockHandle, chunks[firstChunkNum] + (firstChunkLen - remaining), remaining, 0);
            if (n <= 0) break;
            remaining -= n;
        }

        if (!chunks[secondChunkNum]){
            chunks[secondChunkNum] = malloc(sizeof(char) * secondChunkLen);
        }
        chunk_sizes[secondChunkNum] = secondChunkLen;
        remaining = secondChunkLen;
        while (remaining > 0) {
            n = recv(sockHandle, chunks[secondChunkNum] + (secondChunkLen - remaining), remaining, 0);
            if (n <= 0) break;
            remaining -= n;
        }

        shutdown(sockHandle, SHUT_WR);
        close(sockHandle);
    }
    int toWrite = 1;
    int fd;
    for (int i = 0; i < serversActiveWhilePut; i++) {
        if (!chunks[i]) {
            toWrite = 0;
        }
    }
    printf("%s is incomplete\n", filename);
    if (toWrite){
        fd = open(filename, O_WRONLY|O_CREAT|O_TRUNC, 0666);
    } 
    for (int i = 0; i < serversActiveWhilePut; i++) {
        if (toWrite){ 
            write(fd, chunks[i], chunk_sizes[i]);
        }
        free(chunks[i]);
    }
    free(chunk_sizes);
    free(chunks);
    
}