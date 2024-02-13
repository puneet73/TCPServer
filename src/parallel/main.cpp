#include <bits/stdc++.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>

using namespace std;

// Function prototypes
void* serve_helper(void* context);
void parse_command(char* buffer, queue<string>& reqs, pthread_mutex_t& req_m, int buffsize);
void exec_commands(queue<string>& reqs, int soc, unordered_map<string, string>& kv_store, pthread_mutex_t& kv_m);

void serve_client(queue<int>& clients, pthread_mutex_t& client_m, pthread_mutex_t& kv_m, int buffsize) {
    char buffer[buffsize];
    int conn;
    queue<string> requests;

    while (1) {
        pthread_mutex_lock(&client_m);
        if (clients.size() <= 0) {
            pthread_mutex_unlock(&client_m);
            continue;
        }

        cout << "Got a client" << endl;
        conn = clients.front();
        clients.pop();
        pthread_mutex_unlock(&client_m);

        while (fcntl(conn, F_GETFD) != -1) {
            memset(buffer, 0, buffsize);
            read(conn, buffer, buffsize);
            cout << buffer << endl;
            parse_command(buffer, requests, req_m, buffsize);
            exec_commands(requests, conn, kv_store, kv_m);
        }
    }
}

void* serve_helper(void* context) {
    tuple<queue<int>*, pthread_mutex_t*, pthread_mutex_t*, int> params =
        *reinterpret_cast<tuple<queue<int>*, pthread_mutex_t*, pthread_mutex_t*, int>*>(context);

    queue<int>& clients = *get<0>(params);
    pthread_mutex_t& client_m = *get<1>(params);
    pthread_mutex_t& kv_m = *get<2>(params);
    int buffsize = get<3>(params);

    serve_client(clients, client_m, kv_m, buffsize);
    return nullptr;
}

void parse_command(char* buffer, queue<string>& reqs, pthread_mutex_t& req_m, int buffsize) {
    char* p = buffer;
    int mode = 0;
    int i = 0;
    cout << "Parsing Started\n"
         << endl;
    while (*p && i < buffsize) {
        string temp = "";
        while (*p != '\n') {
            i++;
            temp += *p;
            p++;
        }
        p++;
        i++;
        if (temp == "READ") {
            mode = 1;
            temp = "1";
        } else if (temp == "WRITE") {
            mode = 2;
            temp = "2";
        } else if (temp == "COUNT") {
            mode = 3;
            temp = "3";
        } else if (temp == "DELETE") {
            mode = 4;
            temp = "4";
        } else if (temp == "END") {
            mode = 5;
            temp = "5";
        }
        if (mode == 1 || mode == 4) {
            temp += ' ';
            while (*p != '\n') {
                i++;
                temp += *p;
                p++;
            }
            p++;
            i++;
        }
        if (mode == 2) {
            temp += ' ';
            while (*p != '\n') {
                i++;
                temp += *p;
                p++;
            }
            p++;
            i++;
            while (*p != '\n') {
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
    cout << "Parsing Done\n"
         << endl;
}

void exec_commands(queue<string>& reqs, int soc, unordered_map<string, string>& kv_store, pthread_mutex_t& kv_m) {
    string temp;
    string key;
    string val;
    int sock;
    cout << "Execute Started\n";
    // workflow starts
    while (!reqs.empty()) {
        temp = reqs.front();
        reqs.pop();
        pthread_mutex_lock(&kv_m);
        sock = soc;
        if (temp[0] == '1') {
            key = temp.substr(1, temp.size() - 1);
            cout << "read "
                 << "\t" << key << endl;
            if (kv_store.find(key) == kv_store.end()) {
                write(sock, "NULL\n", 5);
            } else
                write(sock, (kv_store[key] + "\n").c_str(), kv_store[key].size() + 1);

        } else if (temp[0] == '2') {
            size_t idx = temp.find(':');
            key = temp.substr(1, idx - 1);
            val = temp.substr(idx + 1, temp.size() - idx - 1);
            cout << "write "
                 << "\t" << key << "\t" << val << endl;
            kv_store[key] = val;
            write(sock, "FIN\n", 4);
        } else if (temp[0] == '3') {
            cout << "count " << endl;
            write(sock, (to_string(kv_store.size()) + "\n").c_str(), (to_string(kv_store.size())).size() + 1);
        } else if (temp[0] == '4') {
            key = temp.substr(1, temp.size() - 1);
            cout << "delete "
                 << "\t" << key << endl;
            if (kv_store.find(key) == kv_store.end()) {
                write(sock, "NULL\n", 5);
            } else
                write(sock, "FIN\n", 4);
        } else if (temp[0] == '5') {
            cout << "end " << endl;
            write(sock, "\n", 1);
            close(sock);
        }
        pthread_mutex_unlock(&kv_m);
        cout << "Execute Done\n";
    }
}

int main(int argc, char** argv) {
    int portno;
    if (argc != 2) {
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        exit(1);
    }
    portno = atoi(argv[1]);

    int SIZE = 8; // You can adjust this value accordingly
    int buffsize = 4096;

    unordered_map<string, string> kv_store;
    queue<int> clients;
    pthread_mutex_t req_m = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t kv_m = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t client_m = PTHREAD_MUTEX_INITIALIZER;
    pthread_t threads[SIZE];

    sockaddr_in server_addr;
    server_addr.sin_port = htons(portno);
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    int sockid = socket(AF_INET, SOCK_STREAM, 0);
    int iSetOption = 1;
    setsockopt(sockid, SOL_SOCKET, SO_REUSEADDR, (char*)&iSetOption, sizeof(iSetOption));

    if (bind(sockid, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        cout << "Error binding, Port might be in use" << endl;
        cerr << "Error " << strerror(errno) << endl;
        exit(0);
    }

    if (listen(sockid, 1024) < 0) {
        cerr << "Error Listening" << endl;
        exit(0);
    }

    for (int i = 0; i < SIZE; i++) {
        tuple<queue<int>*, pthread_mutex_t*, pthread_mutex_t*, int> params = {&clients, &client_m, &kv_m, buffsize};
        if (pthread_create(threads + i, NULL, serve_helper, &params) < 0) {
            cerr << "Error in creating threads" << endl;
            exit(0);
        }
    }

    int conn;
    while (1) {
        sockaddr_in newSockaddr;
        socklen_t socklen = sizeof(newSockaddr);
        conn = accept(sockid, (sockaddr*)&newSockaddr, &socklen);
        cout << "New Client" << endl;
        if (conn < 0) {
            cout << "Error Connecting to client" << endl;
            exit(0);
        }
        pthread_mutex_lock(&client_m);
        clients.push(conn);
        pthread_mutex_unlock(&client_m);
    }

    close(sockid);

    return 0;
}
