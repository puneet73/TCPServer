#include <iostream>
#include <cstdlib>
#include <string>
#include <sstream>
#include <cstring>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <unordered_map>

using namespace std;

const int BUFFER_SIZE = 1024;
unordered_map<string, string> KV_data;
pthread_mutex_t kv_mutex = PTHREAD_MUTEX_INITIALIZER;

void process_connection(int sockfd);

int main(int argc, char **argv)
{
    int port_number;

    if (argc != 2)
    {
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        exit(1);
    }

    port_number = atoi(argv[1]);
    cout << port_number << endl;

    int socket_connect;
    struct sockaddr_in my_addr;

    socket_connect = socket(AF_INET, SOCK_STREAM, 0);

    my_addr.sin_family = AF_INET;
    my_addr.sin_port = htons(port_number);
    bind(socket_connect, (struct sockaddr *)&my_addr, sizeof(struct sockaddr_in));

    listen(socket_connect, 50);

    while (true)
    {
        int socket_accept;
        struct sockaddr_in client_addr;
        socklen_t sz_client = sizeof(client_addr);

        socket_accept = accept(socket_connect, (struct sockaddr *)&client_addr, &sz_client);
        process_connection(socket_accept);
        close(socket_accept);  // Close the accepted socket after processing
    }

    close(socket_connect);
}

void process_connection(int sockfd)
{
    char buffer[BUFFER_SIZE];

    memset(buffer, 0, sizeof(buffer));
    string additional = "";
    string input_s;

    while (true)
    {
        ssize_t bytes_read = read(sockfd, buffer, BUFFER_SIZE - 1); // Ensure null-terminated buffer

        if (bytes_read <= 0)
        {
            break; // Break if no more data or an error occurs
        }

        buffer[bytes_read] = '\0'; // Null-terminate the buffer

        istringstream streamstring(additional + (string)buffer);

        while (true)
        {
            streamstring >> input_s;
            // ...

            pthread_mutex_lock(&kv_mutex); // Lock before accessing shared data
            // ... (perform operations on KV_data)
            pthread_mutex_unlock(&kv_mutex); // Unlock after accessing shared data

            // ...

            if (streamstring.str().length() < 1)
            {
                additional = "";
                break;
            }

            cout << input_s << endl;
        }
    }
}
