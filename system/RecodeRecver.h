#ifndef RECODERECVER_H
#define RECODERECVER_H

#include "global.h"
#include "serialization.h"
#include "ioser.h"
#include "communication.h"
#include "Msg.h"
#include <mpi.h>
#include <string.h>
#include <condition_variable>
#include <mutex>
#include <unistd.h> //for usleep()
using namespace std;

//note:
//basic mode first merge-sorts vertices in increasing order of vid
//later, messages are merge-sorted by tgt-id
//----
//ID-recoding algo builds on basic mode
//recoded mode read from local
//----
//advanced mode (ID-recoding should be done, read from local)
//in-mem agg (requires two sets of states)
//no recv-er file dump, no merge-sort

template <class VertexT>
class RecodeRecver
{
public:
	typedef vector<VertexT> VertexContainer;
	typedef typename VertexT::EdgeType EdgeT;
	typedef typename VertexT::MessageType MessageT;
	typedef typename VertexT::CombinerType CombinerT;
	//----
	typedef IDMsg<VertexT> IDMsgT;

	RecodeRecver() : vertexes(*(VertexContainer *)global_vertices), zero(*(MessageT *)global_zero), combiner(*(CombinerT *)global_combiner) //must be created after RecodeComper
	{
		rstep_before = 0;
		rstep_after = 0;
		job_finished = false;
	}

	//recv a batch
	void recv()
	{
	    MPI_Status status;
		MPI_Recv(glob_recvbuf, MAX_MSG_STREAM_SIZE, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status); //busy waiting if msg is not available
		int len;
		MPI_Get_count(&status, MPI_CHAR, &len);
		if(len > 0)
		{//Case 1: data-msg
			//if rstep_before == 0, we use local load, and no msgs will be received
			if(rstep_before > 0)
			{//iterations for computation
				vector<MessageT>& aggedVec = * (vector<MessageT> *)(global_aggedVec_step[rstep_before + 1]);
				//----
				ibinstream um(glob_recvbuf, len); //create stream from memory buffer directly
				IDMsgT idmsg;
				while(!um.end())
				{
					um >> idmsg;
					int pos = get_vpos(idmsg.id);
					combiner.combine(aggedVec[pos], idmsg.msg);
				}
			}
		}
		else
		{//Case 2: len == 0, endTag
			rcount++;
		}
	}

	//interface to outside
	void run()
	{
		ResetTimer(RECVER_BARRIER_TIMER);
		int has_msg;
		bool term_cond = false;
		while(true)
		{
			//reset
			rcount = 0;
			batNum = 0;
			//---- init agg-ed buf (shared with Comper)
			int tgt_step = rstep_before + 1; //buffer is towards next superstep
			int size_req = tgt_step + 1;
			if(global_aggedVec_step.size() < size_req)
			{
				global_aggedVec_step.resize(size_req);
				global_aggedVec_step[tgt_step] = new vector<MessageT>(vertexes.size(), zero);
			}
			//------
			do{
#ifdef RECVER_POLLING_MODE
				//------ The part below is to avoid busy waiting ------
				MPI_Iprobe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &has_msg, MPI_STATUS_IGNORE);
				while(!has_msg)
				{
					usleep(POLLING_TIME);
					MPI_Iprobe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &has_msg, MPI_STATUS_IGNORE);
				}
				//-----------------------------------------------------
#endif
				recv();
			} while(rcount < _num_workers);
			if(_my_rank == MASTER_RANK) cout<<"- Recver done (for step "<<rstep_before<<")"<<endl;
			//----
			mtx_r2c.lock();
			int prev_rstep = rstep_before;
			rstep_before ++;
			mtx_r2c.unlock();
			cond_r2c.notify_one();
			//----
			//sync between recvers: check termination condition
			//note: since we use tag-free MPI functions, they are not using tag=0 and will not interfere with sender-recver communication
			active_vlock.lock();
			size_t active_count = active_vnum_step[prev_rstep];
			active_vlock.unlock();
			size_t active_vnum = all_sum(active_count);
			if (active_vnum == 0) term_cond = true; //so far, term_cond only considers condition 1
			if(rstep_before > 1)
			{
				//if rstep_before == 1
				//- we ignore force_terminate() (e.g., called inside parseVertex) during loading
				//- we do not check msg-num
				//(we do not check bor_bitmap)
				bor_lock.lock();
				char local_bor = bor_bitmap_step[prev_rstep];
				bor_lock.unlock();
				char bits_bor = all_bor(local_bor);
				if(getBit(FORCE_TERMINATE_ORBIT, bits_bor) == 1) term_cond = true;
				else if (term_cond && getBit(HAS_MSG_ORBIT, bits_bor) == 0) term_cond = true; //first cond is equivalent to "active_vnum == 0"
				else term_cond = false; //now, term_cond considers condition 2 also, and may set term_cond back to false
			}
			else //it is only safe here, glob_sendbuf & glob_recvbuf are note used
			{
				if(senderside_combiner_used)
				{
					int required_size = sizeof(IDMsgT) * global_max_vnum;
					if(required_size > MAX_MSG_STREAM_SIZE)
					{
						MAX_MSG_STREAM_SIZE = required_size;
						delete[] glob_sendbuf;
						delete[] glob_recvbuf;
						glob_sendbuf = new char[required_size];
						glob_recvbuf = new char[required_size];
					}
				}
			}
			StopTimer(RECVER_BARRIER_TIMER);
			string report = "--- --- Recver_Inter-Barrier Time (step = ";
			sprintf(num, "%d", rstep_after.load(memory_order_relaxed));
			report+=num;
			report+=")";
			PrintTimer(report.c_str(), RECVER_BARRIER_TIMER);
			ResetTimer(RECVER_BARRIER_TIMER);
			//----
			//(1)set job_finished to true (lock mtx_r2s, mtx_r2c), (2)notify others about job_finish
			if (term_cond)
			{
				mtx_r2s.lock();
				mtx_r2c.lock();
				job_finished = true;
				mtx_r2s.unlock();
				mtx_r2c.unlock();
				cond_r2s.notify_one();
				cond_r2c.notify_one();
				break;
			}
			//wake up Sender
			mtx_r2s.lock();
			rstep_after ++;
			mtx_r2s.unlock();
			cond_r2s.notify_one();
		}
	}

private:
    int rcount; //to determine whether messages from all workers are received for a superstep
    int batNum; //for a superstep: the next batch_No to mark / also the number of batches currently written
    //--- assisting buffers
    char fname[100]; //buffer for obtaining file name
    char num[20]; //buffer for writing numbers
    //---
    VertexContainer& vertexes;
    MessageT& zero;
    CombinerT& combiner;
};

//potential problem:
//- recv() = recv + sort + write, network IO + CPU + serial disk IO, which can be parallelized to improve performance
//- simplification note: here, since we will run multiple workers per server, parallelization is achieved due to multiple processes

#endif
