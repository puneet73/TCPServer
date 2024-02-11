/*
 * tcpserver.c - A multithreaded TCP echo server
 * usage: tcpserver <port>
 *
 * Testing :
 * nc localhost <port> < input.txt
 */
#include <unistd.h>
#include <iostream>
#include <stdlib.h>
#include <string>
#include <sstream>
#include <cstring>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <bits/stdc++.h>
using namespace std;

unordered_map<string, string> KV_data;
queue<int> queue_request;

pthread_t process[50];

pthread_mutex_t KV;
pthread_mutex_t q;
pthread_cond_t condition;

void process_connection(int);
void *thread_process(void *);

int main(int argc, char **argv)
{
  int port_number; /* port to listen on */
  /*
   * check command line arguments
   */
  if (argc != 2)
  {
    fprintf(stderr, "usage: %s <port>\n", argv[0]);
    exit(1);
  }

  // DONE: Server port number taken as command line argument
  port_number = atoi(argv[1]);

  pthread_mutex_init(&KV, NULL);
  pthread_mutex_init(&q, NULL);

  pthread_cond_init(&condition, NULL);

  for (int i = 0; i < 50; i++)
  {
    pthread_create(&process[i], NULL, thread_process, NULL);
  }

  int socket_accept, socket_connect;
  struct sockaddr_in my_addr, client_addr;

  socket_connect = socket(AF_INET, SOCK_STREAM, 0);

  my_addr.sin_family = AF_INET;
  my_addr.sin_port = htons(port_number);
  bind(socket_connect, (struct sockaddr *)&my_addr, sizeof(struct sockaddr_in));

  listen(socket_connect, 100);

  int client_len = sizeof(client_addr);

  while (true)
  {
    socket_accept = accept(socket_connect, (struct sockaddr *)&client_addr, (socklen_t *)&client_len);
    pthread_mutex_lock(&q);
    queue_request.push(socket_accept);
    pthread_cond_signal(&condition);
    pthread_mutex_unlock(&q);
  }
}

void process_connection(int sockfd)
{

  char buffer[1024];

  memset(buffer, 0, sizeof(buffer));
  string additional = "";
  string input_s;

  while (true)
  {
    read(sockfd, buffer, 1024);
    istringstream streamstring(additional + (string)buffer);

    while (true)
    {
      streamstring >> input_s;
      if (input_s == "READ")
      {
        // cout <<"READ" <<endl;
        streamstring >> input_s;
        pthread_mutex_lock(&KV);
        if (KV_data.find(input_s) == KV_data.end())
        {
          const char *send_d = "NULL\n";
          write(sockfd, send_d, strlen(send_d));
        }
        else
        {

          string se = KV_data[input_s] + "\n";
          const char *send_d = se.c_str();
          write(sockfd, send_d, strlen(send_d));
        }
        pthread_mutex_unlock(&KV);
      }
      else if (input_s == "WRITE")
      {
        streamstring >> input_s;
        string key = input_s;
        streamstring >> input_s;
        input_s = input_s.substr(1, input_s.length() - 1);
        pthread_mutex_lock(&KV);
        KV_data[key] = input_s;
        pthread_mutex_unlock(&KV);
        const char *send_d = "FIN\n";
        write(sockfd, send_d, strlen(send_d));
      }
      else if (input_s == "COUNT")
      {
        // cout <<"COUNT" <<endl;
        pthread_mutex_lock(&KV);
        int count = KV_data.size();
        pthread_mutex_unlock(&KV);
        string se = to_string(count) + "\n";
        const char *send_d = se.c_str();
        write(sockfd, send_d, strlen(send_d));
      }
      else if (input_s == "DELETE")
      {
        // cout <<"DELETE" <<endl;
        streamstring >> input_s;
        pthread_mutex_lock(&KV);
        if (KV_data.find(input_s) == KV_data.end())
        {

          const char *send_d = "NULL\n";
          write(sockfd, send_d, strlen(send_d));
        }
        else
        {
          KV_data.erase(input_s);
          const char *send_d = "FIN\n";
          write(sockfd, send_d, strlen(send_d));
        }
        pthread_mutex_unlock(&KV);
      }
      else if (input_s == "END")
      {
        write(sockfd , "\n", strlen("\n"));
        close(sockfd);
        return;
      }
      else
      {
        if (streamstring.str().length() < 1)
        {
          additional = input_s;
          break;
        }
      }

      if (streamstring.str().length() < 1)
      {
        additional = "";
        break;
      }

      cout << input_s << endl;
    }
  }
}

void *thread_process(void *nothing)
{
  while (true)
  {
    pthread_mutex_lock(&q);
    if (queue_request.size() == 0)
    {
      pthread_cond_wait(&condition, &q);
      pthread_mutex_unlock(&q);
      continue;
    }
    int socket_accept = queue_request.front();
    queue_request.pop();
    pthread_mutex_unlock(&q);

    process_connection(socket_accept);
  }
}
