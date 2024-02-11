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
  int portno; /* port to listen on */
  /*
   * check command line arguments
   */
  if (argc != 2)
  {
    fprintf(stderr, "usage: %s <port>\n", argv[0]);
    exit(1);
  }

  // DONE: Server port number taken as command line argument
  portno = atoi(argv[1]);

  pthread_mutex_init(&KV, NULL);
  pthread_mutex_init(&q, NULL);

  pthread_cond_init(&condition, NULL);

  for (int i = 0; i < 50; i++)
  {
    pthread_create(&process[i], NULL, thread_process, NULL);
  }

  int accept_sock, connection_sock;
  struct sockaddr_in my_addr, client_addr;

  connection_sock = socket(AF_INET, SOCK_STREAM, 0);

  my_addr.sin_family = AF_INET;
  my_addr.sin_port = htons(portno);
  bind(connection_sock, (struct sockaddr *)&my_addr, sizeof(struct sockaddr_in));

  listen(connection_sock, 100);

  int client_len = sizeof(client_addr);

  while (true)
  {
    accept_sock = accept(connection_sock, (struct sockaddr *)&client_addr, (socklen_t *)&client_len);
    pthread_mutex_lock(&q);
    queue_request.push(accept_sock);
    pthread_cond_signal(&condition);
    pthread_mutex_unlock(&q);
  }
}

void process_connection(int sockfd)
{

  char buffer[1024];

  memset(buffer, 0, sizeof(buffer));
  string additional = "";
  string temp;

  while (true)
  {
    read(sockfd, buffer, 1024);
    istringstream streamstring(additional + (string)buffer);

    while (true)
    {
      streamstring >> temp;
      if (temp == "READ")
      {
        // cout <<"READ" <<endl;
        streamstring >> temp;
        pthread_mutex_lock(&KV);
        if (KV_data.find(temp) == KV_data.end())
        {
          const char *sendd = "NULL\n";
          write(sockfd, sendd, strlen(sendd));
        }
        else
        {

          string se = KV_data[temp] + "\n";
          const char *sendd = se.c_str();
          write(sockfd, sendd, strlen(sendd));
        }
        pthread_mutex_unlock(&KV);
      }
      else if (temp == "WRITE")
      {
        streamstring >> temp;
        string key = temp;
        streamstring >> temp;
        temp = temp.substr(1, temp.length() - 1);
        pthread_mutex_lock(&KV);
        KV_data[key] = temp;
        pthread_mutex_unlock(&KV);
        const char *sendd = "FIN\n";
        write(sockfd, sendd, strlen(sendd));
      }
      else if (temp == "COUNT")
      {
        // cout <<"COUNT" <<endl;
        pthread_mutex_lock(&KV);
        int count = KV_data.size();
        pthread_mutex_unlock(&KV);
        string se = to_string(count) + "\n";
        const char *sendd = se.c_str();
        write(sockfd, sendd, strlen(sendd));
      }
      else if (temp == "DELETE")
      {
        // cout <<"DELETE" <<endl;
        streamstring >> temp;
        pthread_mutex_lock(&KV);
        if (KV_data.find(temp) == KV_data.end())
        {

          const char *sendd = "NULL\n";
          write(sockfd, sendd, strlen(sendd));
        }
        else
        {
          KV_data.erase(temp);
          const char *sendd = "FIN\n";
          write(sockfd, sendd, strlen(sendd));
        }
        pthread_mutex_unlock(&KV);
      }
      else if (temp == "END")
      {
        write(sockfd , "\n", strlen("\n"));
        close(sockfd);
        return;
      }
      else
      {
        if (streamstring.str().length() < 1)
        {
          additional = temp;
          break;
        }
      }

      if (streamstring.str().length() < 1)
      {
        additional = "";
        break;
      }

      cout << temp << endl;
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
    int accept_sock = queue_request.front();
    queue_request.pop();
    pthread_mutex_unlock(&q);

    process_connection(accept_sock);
  }
}
