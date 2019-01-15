#ifndef GLOBAL_H
#define GLOBAL_H

#include <mpi.h>
#include <stddef.h>
#include <limits.h>
#include <string>
#include <ext/hash_set>
#include <ext/hash_map>
#define hash_map __gnu_cxx::hash_map
#define hash_set __gnu_cxx::hash_set
#include <sys/stat.h>
#include <mutex>
#include <condition_variable>
#include <string.h>
#include <atomic>
using namespace std;

//============================ new variables for iopregel ============================
#define SKIP_MODE //skip edges of inactive vertices in E-stream?
#define RECVER_POLLING_MODE //sleep for POLLING_TIME when nothing to receive (comment it to busy wait)

//============
#define POLLING_TIME 100 //0.1 ms polling time, used by Recver
#define NUM_WAY_OF_MERGE 1000 //m-way merge sort, the parameter is m
//---
#define MAX_SPLIT_SIZE 8388608 //8M, if more than that, new stream is opened for appending
int MAX_MSG_STREAM_SIZE = 8388608; //8M, may be increased if combiner is used
//note: this is the max allowed size of a msg-stream-file
//      since each worker has at most one sender and one receiver, there are only two communication buffers
//      for simplicity, we set each buffer to have size MAX_MSG_STREAM_SIZE, so each file can be sent in one send-recv pair
char * glob_sendbuf, * glob_recvbuf; //used by sender and receiver
//int * batch_comp; //batch_comp[i] = last batch (to worker i) that gets written //deprecated, replaced by batch_comp_step
vector<atomic<int> *> batch_comp_step; //batch_comp_step[s] = batch_comp[] for superstep s
//batch_comp[] is created by comper, deleted by sender
condition_variable cond_batComp;
mutex mtx_batComp; //lock for batch_comp_step
//-------------------------------------------------------------------------------------
//some important global tags
atomic<int> rstep_before; //written by recv-er before recv-sync
atomic<int> rstep_after; //written by recv-er after recv-sync
//to update, please lock mtx_r2s (due to Sender.wait_for_receiver())
//-- cstep = global_step_num, is used by comp-er
//to update, please lock mtx_c2s (due to Sender.wait_for_computer())
atomic<bool> job_finished; //written by recv-er, set if termination condition holds, and threads will exit
//to update, please lock mtx_r2s (due to Sender.wait_for_receiver())
//-------------------------------------------------------------------------------------
//locks
condition_variable cond_r2s;
mutex mtx_r2s; //recv-er notifies sender to begin next superstep
condition_variable cond_c2s;
mutex mtx_c2s; //comp-er notifies sender to send another file, or to begin next superstep
//to update, please lock mtx_c2s (due to Sender.wait_for_computer())
//int numfiles_comped; //number of files written by comp-er in current superstep //deprecated, replaced by numfiles_comped_step
vector<int> numfiles_comped_step; //numfiles_comped_step[s] = numfiles_comped for superstep s
mutex mtx_r2c; //recv-er notifies comper to begin next superstep
condition_variable cond_r2c;
//-------------------------------------------------------------------------------------
//for deleting outdated msg/adj-list files, usually called async-ly
void fdelete(string fname)
{
	remove(fname.c_str());
}
//globally accessible structures
void * global_vertices; //shared vertex array
bool global_combiner_used = false;
bool global_aggregator_used = false;
//====================================================================================

//============================
///worker info
#define MASTER_RANK 0

int _my_rank;
int _num_workers;

inline int get_worker_id()
{
    return _my_rank;
}

inline int get_num_workers()
{
    return _num_workers;
}

//============================
//for caching iopregel files

void _mkdir(const char *dir) {//taken from: http://nion.modprobe.de/blog/archives/357-Recursive-directory-creation.html
	char tmp[256];
	char *p = NULL;
	size_t len;

	snprintf(tmp, sizeof(tmp), "%s", dir);
	len = strlen(tmp);
	if(tmp[len - 1] == '/') tmp[len - 1] = '\0';
	for(p = tmp + 1; *p; p++)
		if(*p == '/') {
				*p = 0;
				mkdir(tmp, S_IRWXU);
				*p = '/';
		}
	mkdir(tmp, S_IRWXU);
}

string IOPREGEL_SENDER_DIR;
string IOPREGEL_RECVER_DIR;
string IOPREGEL_GRAPH_DIR;
string IOPREGEL_RESULT_DIR;
bool global_hdfs_load;

void init_workers(int argc, char* argv[])
{//local_root is the root of all local files for the job
    int provided;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_size(MPI_COMM_WORLD, &_num_workers);
    MPI_Comm_rank(MPI_COMM_WORLD, &_my_rank);
    //IO-note: init bufs
    glob_sendbuf = new char[MAX_MSG_STREAM_SIZE];
    glob_recvbuf = new char[MAX_MSG_STREAM_SIZE];
}

void worker_finalize()
{
    MPI_Finalize();
    //IO-note: free comm_bufs
	delete[] glob_sendbuf;
	delete[] glob_recvbuf;
}

void worker_barrier()
{
    MPI_Barrier(MPI_COMM_WORLD);
}

//------------------------
// worker parameters

struct WorkerParams {
	string local_root;
    string input_path;
    string output_path;
    bool hdfs_load;
    bool force_write; //only for hdfs
    bool dump_with_edges;
    bool hdfs_dump;
    bool dump_disabled;

    WorkerParams()
    {
    	dump_with_edges = true;
    	hdfs_load = true;
    	hdfs_dump = true;
    	force_write = true;
    	dump_disabled = false;
    	local_root = "/tmp/iopregel";
    	output_path = "/tmp/iopregel/output"; //default output path on localFS
    }
};

//============================
//general types
typedef long long VertexID;

//============================
//global variables
atomic<int> global_step_num;
inline int step_num()
{
    return global_step_num;
}

/* //decided not to implement aggregator, since it needs recver-sync, and thus will delay the start of computation
void* global_aggregator = NULL;
inline void set_aggregator(void* ag)
{
    global_aggregator = ag;
}
inline void* get_aggregator()
{
    return global_aggregator;
}
*/

void* global_agg = NULL; //for aggregator, FinalT of last round
inline void* getAgg()
{
    return global_agg;
}

/* no longer used
int global_vnum = 0;
inline int& get_vnum()
{
    return global_vnum;
}
*/

//============================ below are made superstep sensitive ============================
/*
int global_active_vnum = 0;
inline int& active_vnum()
{
    return global_active_vnum;
}
*/
mutex active_vlock;
vector<size_t> active_vnum_step; //written by Comper, read by Recver
//step 0: written by Recver
//step > 0: written by Comper

enum BITS {
    HAS_MSG_ORBIT = 0,
    FORCE_TERMINATE_ORBIT = 1
};

/*
char global_bor_bitmap;

void clearBits()
{
    global_bor_bitmap = 0;
}

void setBit(int bit)
{
    global_bor_bitmap |= (1 << bit);
}

int getBit(int bit, char bitmap)
{
    return ((bitmap & (1 << bit)) == 0) ? 0 : 1;
}

void hasMsg()
{
    setBit(HAS_MSG_ORBIT);
}

void forceTerminate()
{
    setBit(FORCE_TERMINATE_ORBIT);
}
*/

vector<char> bor_bitmap_step;
mutex bor_lock;

void clearBits() //must be called by Comper
{
	unique_lock<mutex> lk(bor_lock);
	//now we first create the space
	int size_req = global_step_num + 1;
	if(bor_bitmap_step.size() < size_req) bor_bitmap_step.resize(size_req);
	//---
	bor_bitmap_step[global_step_num] = 0;
}

void setBit(int bit) //must be called by Comper
{
	unique_lock<mutex> lk(bor_lock);
	bor_bitmap_step[global_step_num] |= (1 << bit);
}

int getBit(int bit, char bitmap) //called by Recver
{
	return ((bitmap & (1 << bit)) == 0) ? 0 : 1;
}

void hasMsg() //must be called by Comper
{
    setBit(HAS_MSG_ORBIT);
}

void forceTerminate() //must be called by Comper
{
    setBit(FORCE_TERMINATE_ORBIT);
}

//============================ new structures for recoded-mode
void * global_combiner; //set by RecodeSender
//must be used in recoded-mode
void * global_zero; //the identity of type MessageT //set by RecodeComper
//---
vector<void *> global_aggedVec_step; //global_valueVec is step sensitive
//created by RecodeRecver, freed by RecodeComper
//---
size_t global_max_vnum; //maximum number of vertices on any machine
//set by RecodeRecver, when resizing buffers
//---
int get_vpos(VertexID vid) //convert from vid to vpos-in-varray
{
	return vid / _num_workers;
}
//---
bool senderside_combiner_used = false;

#endif
