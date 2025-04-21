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
#include <dirent.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>
/*
    Fixes: 
        When putting a file, will currently append to a file if already created.
        This means putting the same file twice will create a weird double frankenstein copy
        Need to figure out how to delete and recreate the file if it already exists on the server

    
*/
#define MAX_RECEIVE_BUFFER 2048
#define MAX_CLIENTS 100
#define ERROR -1
atomic_int countActiveThreads;
char* directory;
void killHandler(int sig){
    signal(sig, SIG_IGN);
    printf("\nFinishing serving clients...\n");
    printf("There are currently %d clients with an ongoing connection\n", countActiveThreads);
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
void clearCache(const char *dirPath) {
    DIR *dir = opendir(dirPath);
    if (!dir) {
        perror("opendir failed");
        return;
    }

    struct dirent *entry;
    char filePath[512];

    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;

        // Construct full file path
        snprintf(filePath, sizeof(filePath), "%s/%s", dirPath, entry->d_name);

        if (remove(filePath) != 0) {
            perror("remove failed");
        }
    }

    closedir(dir);
}
void* serveClient(void* args) {
    int clientSock = *(int*)args;
    free(args);

    // 1) Track active threads
    atomic_fetch_add(&countActiveThreads, 1);

    char buf[MAX_RECEIVE_BUFFER];
    ssize_t n;

    while (1) {
        // 2) Read one command line (up to newline)
        n = recv(clientSock, buf, MAX_RECEIVE_BUFFER-1, 0);
        if (n <= 0) {break;}

        buf[n] = '\0';

        // 3) Tokenize: command [filename] [chunkPair] [length]\n
        char *cmd  = strtok(buf, " \n");
        char *fn   = strtok(NULL, " \n");
        char *pair = strtok(NULL, " \n");
        char *lenS = strtok(NULL, " \n");
        int   length = lenS ? atoi(lenS) : 0;

        if (!cmd) continue;

        if (strcmp(cmd, "put") == 0) {
            // read exactly `length` bytes and write into directory/fn.pair
            char path[512];
            snprintf(path, sizeof(path), "%s/%s.%s", directory, fn, pair);
            int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0666);
            if (fd < 0) {
                printf("Path: %s\n", path);
                perror("open for put");
                send(clientSock, "ERROR\n", 6, 0);
            } else {
                int remaining = length;
                send(clientSock, "\n", 1, 0);
                while (remaining > 0) {
                    n = recv(clientSock, buf, 
                             remaining > MAX_RECEIVE_BUFFER-1 ? MAX_RECEIVE_BUFFER-1 : remaining, 0);
                    if (n <= 0) break;
                    buf[MAX_RECEIVE_BUFFER-1] = '\0';
                    write(fd, buf, n);
                    remaining -= n;
                }
                close(fd);
                send(clientSock, "OK\n", 3, 0);
            }
        }
        else if (strcmp(cmd, "get") == 0) {
            // open directory/fn.pair, stat to find size, send “DATA size\n” then content
            char path[512];
            snprintf(path, sizeof(path), "%s/%s.%s", directory, fn, pair);
            struct stat st;
            if (stat(path, &st) < 0) {
                send(clientSock, "ERROR\n", 6, 0);
            } else {
                // tell client how many bytes
                char header[64];
                int sz = st.st_size;
                int hlen = snprintf(header, sizeof(header), "DATA %d\n", sz);
                send(clientSock, header, hlen, 0);

                // stream file
                int fd = open(path, O_RDONLY);
                while ((n = read(fd, buf, MAX_RECEIVE_BUFFER)) > 0) {
                    send(clientSock, buf, n, 0);
                }
                close(fd);
            }
        }
        else if (strcmp(cmd, "list") == 0) {
            // scan the server’s directory and report all “filename.pair” entries
            DIR *d = opendir(directory);
            struct dirent *entry;
            if (!d) {
                send(clientSock, "ERROR\n", 6, 0);
            } else {
                while ((entry = readdir(d)) != NULL) {
                    if (entry->d_type == DT_REG) {
                        // send each chunk name back
                        if (send(clientSock, entry->d_name,strlen(entry->d_name), 0) < 0) {
                            perror("[SERVER] Error in send");
                            close(clientSock);
                            return NULL;
                        };
                        if (send(clientSock, "\n", 1, 0) < 0) {
                            perror("[SERVER] Error in send");
                            close(clientSock);
                            return NULL;
                        };
                    }
                }
                closedir(d);
                if (send(clientSock, "END\n", 4, 0) < 0) {
                    perror("[SERVER] Error in send");
                    close(clientSock);
                    return NULL;
                };
            }
        }
        else {
            // unknown command
            send(clientSock, "ERROR Unknown command\n", 21, 0);
        }
    }

    // 4) Cleanup
    close(clientSock);
    atomic_fetch_sub(&countActiveThreads, 1);
    return NULL;
}
int main(int argc, char **argv){
    if (argc < 3){
        printf("Incorrect Number of Args! Run with ./dfs [Directory] [Port Number]\n");
        exit(-1);
    }
    directory = argv[1];
    clearCache(directory);
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
        // printf("\n\nHandling Client Connected from port no %d and IP %s\n",ntohs(client.sin_port), ip_str);
    }
    close(sock);
}
