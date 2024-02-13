#include <pthread.h>
#include <arpa/inet.h>
#include <string.h>
#include <iostream>
#include <unordered_map>
#include <queue>
#include <vector>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <cstring>
#include <pthread.h>
#include <signal.h>
using namespace std;

class Server{
    public:
        int PORT;
        int SIZE;
        std::string IP;
        Server(int port, int size){
            PORT = port;
            SIZE = size;
            req_m = PTHREAD_MUTEX_INITIALIZER;
            kv_m = PTHREAD_MUTEX_INITIALIZER;
            client_m = PTHREAD_MUTEX_INITIALIZER;
            threads = (pthread_t*)malloc(sizeof(pthread_t)*SIZE);
            sockaddr_in server_addr;
            server_addr.sin_port = htons(PORT);
            server_addr.sin_family = AF_INET;
            server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

            sockid = socket(AF_INET, SOCK_STREAM, 0);
            int iSetOption = 1;
            setsockopt(sockid, SOL_SOCKET, SO_REUSEADDR, (char*)&iSetOption,sizeof(iSetOption));
            if(bind(sockid, (sockaddr*)&server_addr, sizeof(server_addr)) < 0){
                std::cout << "Error binding, Port might be in use" << std::endl;
                std::cerr << "Error " << std::strerror(errno) << std::endl;
                exit(0); 
            }

            if(listen(sockid, 1024) < 0){
                std::cerr << "Error Listening" << std::endl;
                exit(0);
            }
            for(int i=0; i<SIZE; i++){
                if(pthread_create(threads+i, NULL, &Server::serve_helper, this) < 0){
                    std::cerr << "Error in creating threads" << std::endl;
                    
                    exit(0);
                }
            }
            Add_request();
            close(sockid);
            // for(int i=0; i<SIZE; i++){
            //     pthread_join(threads[i], NULL);
            // }
        }

        int Add_request(){
            int conn;
            while(1){
                sockaddr_in newSockaddr;
                socklen_t socklen = sizeof(newSockaddr);
                conn = accept(sockid, (sockaddr*)&newSockaddr, &socklen); 
                std::cout << "New Client" << std::endl;
                if(conn < 0){
                    std::cout << "Error Connecting to client" << std::endl;
                    exit(0);
                }
                pthread_mutex_lock(&client_m);
                clients.push(conn);
                pthread_mutex_unlock(&client_m);
                
            }
        }

    private:
        std::unordered_map<std::string, std::string> kv_store;
        std::queue<int> clients;
        pthread_mutex_t req_m, kv_m, client_m;
        pthread_t* threads;
        
        void parse_command(char* buffer, std::queue<std::string>& reqs){
            char* p = buffer;
            int mode = 0; // UNDEFINED
            /*
                1 -> Read
                2 -> Write
                3 -> Count
                4 -> Delete
                5 -> End
            */
            int i=0;
            std::cout << "Parsing Started\n" << std::endl;
            while(*p && i<buffsize){
                std::string temp = "";
                while(*p != '\n'){
                    i++;
                    temp += *p;
                    p++;
                }
                p++;
                i++;
                if(temp == "READ"){
                    mode = 1;
                    temp = "1";
                }
                else if(temp == "WRITE"){
                    mode = 2;
                    temp = "2";
                }
                else if(temp == "COUNT"){
                    mode = 3;
                    temp = "3";
                }
                else if(temp == "DELETE"){
                    mode = 4;
                    temp = "4";
                }
                else if(temp == "END"){
                    mode = 5;
                    temp = "5";
                }
                if(mode == 1 || mode == 4){
                    temp += ' ';
                    while(*p != '\n'){
                        i++;
                        temp += *p;
                        p++;
                    }
                    p++;
                    i++;
                }
                if(mode == 2){
                    temp += ' ';
                    while(*p != '\n'){
                        i++;
                        temp += *p;
                        p++;
                    }
                    p++;
                    i++;
                    while(*p != '\n'){
                        i++;
                        temp += *p;
                        p++;
                    }
                    p++;
                    i++;
                }
                reqs.push(temp); 
                
            }
            buffer[i] = 0;
            pthread_mutex_unlock(&req_m);
            std::cout << "Parsing Done\n" << std::endl;
        }
        void exec_commands(std::queue<std::string>& reqs, int soc){
            std::string temp;
            std::string key;
            std::string val;
            int sock;
            std::cout << "Execute Started\n";
            // Comment for wf
            while(!reqs.empty()){
                temp = reqs.front();
                reqs.pop();
                pthread_mutex_lock(&kv_m);
                sock = soc;
                if(temp[0] == '1'){
                    key = temp.substr(1, temp.size() -1);
                    std::cout << "read " << "\t" << key << std::endl;
                    if(kv_store.find(key) == kv_store.end()){
                        write(sock, "NULL\n", 5);
                    }
                    else write(sock, (kv_store[key]+"\n").c_str(), kv_store[key].size()+1);

                }
                else if(temp[0] == '2'){
                    size_t idx = temp.find(':');
                    key = temp.substr(1, idx-1);
                    val = temp.substr(idx+1, temp.size() - idx -1);
                    std::cout << "write " << "\t" << key << "\t" << val << std::endl;
                    kv_store[key] = val;
                    write(sock, "FIN\n", 4);
                }

                else if(temp[0] == '3'){
                    std::cout << "count " << std::endl;
                    write(sock, (std::to_string(kv_store.size())+"\n").c_str(), (std::to_string(kv_store.size())).size()+1);
                }
                else if(temp[0] == '4'){
                    key = temp.substr(1, temp.size() - 1);
                    std::cout << "delete " << "\t" << key << std::endl;
                    if(kv_store.find(key) == kv_store.end()){
                        write(sock, "NULL\n", 5);
                    }
                    else write(sock, "FIN\n", 4);
                }
                else if(temp[0] == '5'){
                    std::cout << "end " << std::endl;
                    write(sock, "\n", 1);
                    close(sock);
                }
                pthread_mutex_unlock(&kv_m);
                std::cout << "Execute Done\n";
            }
            
        }

        void serve_client(){
            char buffer[buffsize];
            int conn;
            std::queue<std::string> requests;
            while(1){
                pthread_mutex_lock(&client_m);
                if(clients.size() <= 0){
                    pthread_mutex_unlock(&client_m);
                    continue;
                }
                std::cout << "Got a client" << std::endl;
                conn = clients.front();
                clients.pop();
                pthread_mutex_unlock(&client_m);
                while(fcntl(conn, F_GETFD) != -1){
                    memset(buffer, 0, buffsize);
                    read(conn, buffer, buffsize);
                    std::cout << buffer << std::endl;
                    parse_command(buffer, requests);
                    exec_commands(requests, conn);
                }
            }
        }

        static void* serve_helper(void* context){
            Server* c = static_cast<Server*>(context);
            c->serve_client();
            return nullptr;
        }

        int sockid;
        int buffsize = 4096;
};
int main(int argc, char ** argv) {
  int portno; /* port to listen on */
  
  /* 
   * check command line arguments 
   */
  if (argc != 2) {
    fprintf(stderr, "usage: %s <port>\n", argv[0]);
    exit(1);
  }

  // DONE: Server port number taken as command line argument
  portno = atoi(argv[1]);
  Server server(portno, 8);
}
