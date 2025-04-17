#include <stdio.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <stdatomic.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <signal.h>
#include <strings.h>
#include <pthread.h>
#define MAX_RECEIVE_BUFFER 2048
#define MAX_CLIENTS 100
#define ERROR -1
atomic_int countActiveThreads;
char* directory;
void killHandler(int sig){
    signal(sig, SIG_IGN);
    printf("\nFinishing serving clients...\n");
    printf("There are currently %d clients with a persistant connection\n", countActiveThreads);
    int cycles = 0;
    while (countActiveThreads >= 1){
        sleep(1);
        if (cycles >= 10){
            exit(1);
        }
        cycles+=1;
    }
    exit(1);
}
void* serveClient(void* args){

}
int main(int argc, char **argv){
    if (argc < 3){
        printf("Incorrect Number of Args! Run with ./dfs [Directory] [Port Number]\n");
        exit(-1);
    }
    directory = argv[1];
    struct sockaddr_in server;
    struct sockaddr_in client;
    int sock;
    int newClientSock;
    unsigned int socketaddr_len = sizeof(struct sockaddr_in);
    int port = atoi(argv[2]);

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) == ERROR){
        perror("Error in socket : ");
        exit(-1);
    }
    int op = 1;

    if (setsockopt(sock, SOL_SOCKET,SO_REUSEADDR, &op, sizeof(op)) < 0){
        perror("Error setting sock op: ");
        exit(-1);
    }
    server.sin_family = AF_INET;
    server.sin_port = htons(port);
    server.sin_addr.s_addr = INADDR_ANY;
    bzero(&server.sin_zero,8);
    if ((bind(sock, (struct sockaddr* )&server, socketaddr_len)) == ERROR){
        perror("Error in bind : ");
        exit(-1);
    }

    if ((listen(sock, MAX_CLIENTS)) == -1){
        perror("Error in listen : ");
        exit(-1);
    }
    char ip_str[INET_ADDRSTRLEN];
    countActiveThreads = 0;

    signal(SIGINT, killHandler);
    while (1){
        if ((newClientSock = accept(sock, (struct sockaddr *) &client, &socketaddr_len)) == ERROR){
            perror("Error in accept : ");
            exit(-1);
        }
        if (inet_ntop(AF_INET, &client.sin_addr, ip_str, sizeof(ip_str)) == NULL) {
            perror("inet_ntop error");
        }
        struct timeval timeout;      
        timeout.tv_sec = 10;
        timeout.tv_usec = 0;
        if (setsockopt (newClientSock, SOL_SOCKET, SO_RCVTIMEO, &timeout,sizeof timeout) < 0 || setsockopt (newClientSock, SOL_SOCKET, SO_SNDTIMEO, &timeout,sizeof timeout) < 0){
            close(newClientSock);
            perror("setsockopt failed ");
        }
        else{
            int* newClientSocket = (int*) malloc(sizeof(int));
            *newClientSocket = newClientSock;
            void* data = newClientSocket;
            pthread_t ptid;
            pthread_create(&ptid, NULL, &serveClient, data);
            pthread_detach(ptid);
        }
        printf("\n\nHandling Client Connected from port no %d and IP %s\n",ntohs(client.sin_port), ip_str);
    }
    close(sock);
}
