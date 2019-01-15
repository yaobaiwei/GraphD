#ifndef COMMUNICATION_H
#define COMMUNICATION_H

#include <mpi.h>
#include "serialization.h"
#include "global.h"

//MPI collective communication primitives do not take “tag” as an input, so they can conflict with peer-to-peer (p2p) primitives and other collective primitives when multithreading is used and threads all send messages (see MPI_Init_thread(.) at “global.h” Line 122). So we avoid using that, and only use P2P operations to implement collective primitives ourselves

//============================================
//char-level send/recv
void pregel_send(void* buf, int size, int dst, int channel)
{
    MPI_Send(buf, size, MPI_CHAR, dst, channel, MPI_COMM_WORLD);
}

void pregel_recv(void* buf, int size, int src, int channel)
{
    MPI_Recv(buf, size, MPI_CHAR, src, channel, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

//------
//binstream-level send
void send_obinstream(obinstream& m, int dst, int channel)
{
    size_t size = m.size();
    pregel_send(&size, sizeof(size_t), dst, channel);
    pregel_send(m.get_buf(), m.size(), dst, channel);
}

ibinstream recv_ibinstream(int src, char* buf, int channel)
{
    size_t size;
    pregel_recv(&size, sizeof(size_t), src, channel);
    buf = new char[size];
    pregel_recv(buf, size, src, channel);
    return ibinstream(buf, size);
}

//------
//obj-level send/recv
template <class T>
void send_data(const T& data, int dst, int channel)
{
    obinstream m;
    m << data;
    send_obinstream(m, dst, channel);
}

template <class T>
T recv_data(int src, int channel)
{
    size_t size;
    char* buf = new char[size];
    ibinstream um = recv_ibinstream(src, buf, channel);
    T data;
    um >> data;
    delete[] buf;
    return data;
}

//============================================
#define COMM_CHANNEL_RECV 101

//Allreduce
size_t all_max(size_t my_copy)
{
	size_t max;
    if(_my_rank == 0) //we use the knowledge that 0 is MASTER_RANK
    {
        max = my_copy;
        for(int i=1; i<_num_workers; i++)
        {
        	size_t temp = recv_data<size_t>(i, COMM_CHANNEL_RECV);
            if(temp > max) max = temp;
        }
        for(int i=1; i<_num_workers; i++)
        {
            send_data(max, i, COMM_CHANNEL_RECV);
        }
    }
    else
    {
        send_data(my_copy, 0, COMM_CHANNEL_RECV);
        max = recv_data<size_t>(0, COMM_CHANNEL_RECV);
    }
    return max;
}

size_t all_sum(size_t my_copy)
{
	size_t sum;
    if(_my_rank == 0) //we use the knowledge that 0 is MASTER_RANK
    {
        sum = my_copy;
        for(int i=1; i<_num_workers; i++)
        {
            sum += recv_data<size_t>(i, COMM_CHANNEL_RECV);
        }
        for(int i=1; i<_num_workers; i++)
        {
            send_data(sum, i, COMM_CHANNEL_RECV);
        }
    }
    else
    {
        send_data(my_copy, 0, COMM_CHANNEL_RECV);
        sum = recv_data<size_t>(0, COMM_CHANNEL_RECV);
    }
    return sum;
}

char all_bor(char my_copy)
{
    char sum;
    if(_my_rank == 0) //we use the knowledge that 0 is MASTER_RANK
    {
        sum = my_copy;
        for(int i=1; i<_num_workers; i++)
        {
            sum |= recv_data<char>(i, COMM_CHANNEL_RECV);
        }
        for(int i=1; i<_num_workers; i++)
        {
            send_data(sum, i, COMM_CHANNEL_RECV);
        }
    }
    else
    {
        send_data(my_copy, 0, COMM_CHANNEL_RECV);
        sum = recv_data<char>(0, COMM_CHANNEL_RECV);
    }
    return sum;
}

//============================================
//scatter
template <class T>
void masterScatter(vector<T>& to_send)
{ //scatter
    int* sendcounts = new int[_num_workers];
    int recvcount;
    int* sendoffset = new int[_num_workers];

    obinstream m;
    int size = 0;
    for (int i = 0; i < _num_workers; i++) {
        if (i == _my_rank) {
            sendcounts[i] = 0;
        } else {
            m << to_send[i];
            sendcounts[i] = m.size() - size;
            size = m.size();
        }
    }

    MPI_Scatter(sendcounts, 1, MPI_INT, &recvcount, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);

    for (int i = 0; i < _num_workers; i++) {
        sendoffset[i] = (i == 0 ? 0 : sendoffset[i - 1] + sendcounts[i - 1]);
    }
    char* sendbuf = m.get_buf(); //obinstream will delete it
    char* recvbuf;

    MPI_Scatterv(sendbuf, sendcounts, sendoffset, MPI_CHAR, recvbuf, recvcount, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);

    delete[] sendcounts;
    delete[] sendoffset;
}

template <class T>
void slaveScatter(T& to_get)
{ //scatter
    int* sendcounts;
    int recvcount;
    int* sendoffset;

    MPI_Scatter(sendcounts, 1, MPI_INT, &recvcount, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);

    char* sendbuf;
    char* recvbuf = new char[recvcount];

    MPI_Scatterv(sendbuf, sendcounts, sendoffset, MPI_CHAR, recvbuf, recvcount, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);

    ibinstream um(recvbuf, recvcount);
    um >> to_get;
    delete[] recvbuf;
}

//================================================================
//gather
template <class T>
void masterGather(vector<T>& to_get)
{ //gather
    int sendcount = 0;
    int* recvcounts = new int[_num_workers];
    int* recvoffset = new int[_num_workers];

    MPI_Gather(&sendcount, 1, MPI_INT, recvcounts, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);

    for (int i = 0; i < _num_workers; i++) {
        recvoffset[i] = (i == 0 ? 0 : recvoffset[i - 1] + recvcounts[i - 1]);
    }

    char* sendbuf;
    int recv_tot = recvoffset[_num_workers - 1] + recvcounts[_num_workers - 1];
    char* recvbuf = new char[recv_tot];

    MPI_Gatherv(sendbuf, sendcount, MPI_CHAR, recvbuf, recvcounts, recvoffset, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);

    ibinstream um(recvbuf, recv_tot);
    for (int i = 0; i < _num_workers; i++) {
        if (i == _my_rank)
            continue;
        um >> to_get[i];
    }
    delete[] recvcounts;
    delete[] recvoffset;
    delete[] recvbuf;
}

template <class T>
void slaveGather(T& to_send)
{ //gather
    int sendcount;
    int* recvcounts;
    int* recvoffset;

    obinstream m;
    m << to_send;
    sendcount = m.size();

    MPI_Gather(&sendcount, 1, MPI_INT, recvcounts, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);

    char* sendbuf = m.get_buf(); //obinstream will delete it
    char* recvbuf;

    MPI_Gatherv(sendbuf, sendcount, MPI_CHAR, recvbuf, recvcounts, recvoffset, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
}

//================================================================
//bcast
template <class T>
void masterBcast(T& to_send)
{ //broadcast
    obinstream m;
    m << to_send;
    int size = m.size();

    MPI_Bcast(&size, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);

    char* sendbuf = m.get_buf();
    MPI_Bcast(sendbuf, size, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
}

template <class T>
void slaveBcast(T& to_get)
{ //broadcast
    int size;

    MPI_Bcast(&size, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);

    char* recvbuf = new char[size];
    MPI_Bcast(recvbuf, size, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);

    ibinstream um(recvbuf, size);
    um >> to_get;

    delete[] recvbuf;
}

//============================================
#define COMM_CHANNEL_100 100//All are sent along channel 100, should not conflict with others
//char-level send/recv
void pregel_send(void* buf, int size, int dst)
{
    MPI_Send(buf, size, MPI_CHAR, dst, COMM_CHANNEL_100, MPI_COMM_WORLD);
}

void pregel_recv(void* buf, int size, int src)
{
    MPI_Recv(buf, size, MPI_CHAR, src, COMM_CHANNEL_100, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

//------
//binstream-level send
void send_obinstream(obinstream& m, int dst)
{
    size_t size = m.size();
    pregel_send(&size, sizeof(size_t), dst);
    pregel_send(m.get_buf(), m.size(), dst);
}

ibinstream recv_ibinstream(int src, char* buf)
{
    size_t size;
    pregel_recv(&size, sizeof(size_t), src);
    buf = new char[size];
    pregel_recv(buf, size, src);
    return ibinstream(buf, size);
}

//------
//obj-level send/recv
template <class T>
void send_data(const T& data, int dst)
{
	obinstream m;
    m << data;
    send_obinstream(m, dst);
}

template <class T>
T recv_data(int src)
{
	size_t size;
	char* buf = new char[size];
	ibinstream um = recv_ibinstream(src, buf);
    T data;
    um >> data;
    delete[] buf;
    return data;
}

#endif
