#include <bits/stdc++.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>


using namespace std;
class Server{
    public:
        int PORT;
        std::string IP;
        Server(int port){
            PORT = port;
            sockaddr_in server_addr;
            server_addr.sin_port = htons(PORT);
            server_addr.sin_family = AF_INET;
            server_addr.sin_addr.s_addr = INADDR_ANY;

            sockid = socket(AF_INET, SOCK_STREAM, 0);
            int iSetOption = 1;
            setsockopt(sockid, SOL_SOCKET, SO_REUSEADDR, (char*)&iSetOption,sizeof(iSetOption));
            if(bind(sockid, (sockaddr*)&server_addr, sizeof(server_addr)) < 0){
                std::cerr << "Error binding, Port might be in use" << std::endl;
                exit(0); 
            }

            if(listen(sockid, 5) < 0){
                std::cerr << "Error Listening" << std::endl;
                exit(0);
            }
            
            Add_request();
            close(sockid);
        }

        int Add_request(){
            int conn;
            char buffer[buffsize];
            while(1){
                conn = accept(sockid, nullptr, nullptr); 
                if(conn < 0){
                    std::cerr << "Error Connecting to client" << std::endl;
                    exit(0);
                }
                while(fcntl(conn, F_GETFD) != -1){
                    memset(buffer, 0, buffsize);
                    read(conn, buffer, buffsize);
                    parse_command(buffer);
                    exec_commands(conn);
                }
                
            }
        }

    private:
        std::unordered_map<std::string, std::string> kv_store;
        std::queue<std::string> requests;
        void parse_command(char* buffer){
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
                requests.push(temp); 
            }
            buffer[i] = 0;
            std::cout << "Parsing Done\n" << std::endl;
        }
        void exec_commands(int sock){
            std::string temp = "";
            std::string key;
            std::string val;
            std::cout << "Execute Started\n";
            while(!requests.empty()){
                temp = requests.front();
                requests.pop();
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
                else{
                    std::cout << "end " << std::endl;
                    write(sock, "\n", 1);
                    close(sock);
                }

            }
            std::cout << "Execute Done\n";
        }
        int sockid;
        int buffsize = 1024;
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

  Server server(portno);

}
