#ifndef RECODESENDER_H
#define RECODESENDER_H

#include "global.h"
#include "Combiner.h"
#include <mpi.h>
#include <string.h>
#include <sys/stat.h>
#include <fstream>
#include <condition_variable>
#include <mutex>
#include <iostream>
#include <typeinfo>
#include <typeindex>
using namespace std;

template <class VertexT>
class RecodeSender {
public:
	typedef typename VertexT::MessageType MessageT;
	typedef typename VertexT::CombinerType CombinerT;
	typedef IDMsg<VertexT> IDMsgT;

	void wait_for_batchComp() //wait till batchComp[sstep] is ready for scanning
	{
		unique_lock<mutex> lock_batComp(mtx_batComp);
		while((int)batch_comp_step.size() - 1 < sstep) cond_batComp.wait(lock_batComp); //if case 1 holds, numfiles_comped is no longer valid, but will not be checked anyway
		sender_batch_comp = batch_comp_step[sstep]; //update batch_comp
	}

	void clear()//done at the beginning of a superstep
	{
		for(int i=0; i<_num_workers; i++) batch_sent[i] = -1; //start from sending batch_0
		numfiles_sent = 0;
	}

	RecodeSender(bool enable_senderside_combine) : zero(*(MessageT *)global_zero) //must be created after RecodeComper
	{
		senderside_combiner_used = enable_senderside_combine;
		cur_pos = _my_rank; //so that it is different for every one
		batch_sent = new int[_num_workers];
		sstep = 0;
		clear();
		//---
		global_combiner_used = true;//combiner must be used in recoded mode
		//copy combiner info to global structures, to be used by RecodeRecver
		global_combiner = &combiner;
	}

	~RecodeSender()
	{
		delete[] batch_sent;
	}

    //================================= sending logic

	//set filename for a computed batch
	void set_fname_computed(int batch)
	{
		strcpy(fname, IOPREGEL_SENDER_DIR.c_str());
    	sprintf(num, "/%d_", sstep);
    	strcat(fname, num);
    	sprintf(num, "%d_", _my_rank);
    	strcat(fname, num);
    	sprintf(num, "%d_", cur_pos);
		strcat(fname, num);
		sprintf(num, "%d", batch);
		strcat(fname, num);
	}

    //- assisting function
	//to send a batch, used when combiner is not used
    void send_a_batch()
    {//send file "step_src_tgt_batch"
    	batch_sent[cur_pos]++; // the batch to send
    	set_fname_computed(batch_sent[cur_pos]);
		//till now, fname is set
		//get file size
		struct stat statbuf;
		stat(fname, &statbuf);
		off_t fsize = statbuf.st_size;
		if(fsize == 0)
		{//this may happen when disk is full
			cout<<"[WARNING] File of 0 size is read by sender !!! this may happen when disk is full";
			MPI_Abort(MPI_COMM_WORLD, 1);
		}
		//read whole file
		ifstream in(fname);
		in.read(glob_sendbuf, fsize);
		in.close();
		remove(fname);
		//send to tgt
		MPI_Send(glob_sendbuf, fsize, MPI_CHAR, cur_pos, 0, MPI_COMM_WORLD);
		//------
		numfiles_sent ++;
    }

    void combine_and_send_batches()
	{
    	//init buffer to combine
    	if(aggbuf.size() < global_max_vnum) aggbuf.resize(global_max_vnum, zero);
    	//---
    	int batch1 = batch_sent[cur_pos] + 1;
    	int batch2 = sender_batch_comp[cur_pos];
    	IDMsgT tmp;
    	int pos;
    	for(int i=batch1; i<=batch2; i++)
    	{
    		//aggregate batch i
    		set_fname_computed(i); //set file name
			ifbinstream in(fname);
			while(!in.eof())
			{
				in >> tmp;
				pos = get_vpos(tmp.id);
				combiner.combine(aggbuf[pos], tmp.msg);
			}
			in.close();
			remove(fname);
    	}
    	//send combined msgs
    	obinstream combined;
    	for(int i=0; i<aggbuf.size(); i++)
    	{
    		if(aggbuf[i] != zero)
    		{//need to send
    			VertexID tgt_id = _num_workers * i + cur_pos;
    			combined << tgt_id << aggbuf[i];
    			aggbuf[i] = zero; //set back
    		}
    	}
    	MPI_Send(combined.get_buf(), combined.size(), MPI_CHAR, cur_pos, 0, MPI_COMM_WORLD);
    	//update control variables
    	numfiles_sent += batch2 - batch_sent[cur_pos];
    	batch_sent[cur_pos] = batch2;
	}

    void send()
	{
    	if(!senderside_combiner_used || sstep == 0) send_a_batch();
		else combine_and_send_batches();
	}

    //- assisting function
	//notify the end of msg-sending of a superstep towards tgt
	inline void send_endTag(int tgt)
	{
		MPI_Send(NULL, 0, MPI_CHAR, tgt, 0, MPI_COMM_WORLD); //send empty msg
		batch_sent[tgt] = -2;
	}

    //- assisting function
    //to scan for a round
    //- return false if nothing to send
    //- return true otherwise, and set cur_pos properly
    bool scan()
    {
    	bool comp_done = (sstep < global_step_num);
    	if(comp_done)
    	{
			int tgt = cur_pos;
			int cnt = 0;
			while(batch_sent[tgt] == -2 || sender_batch_comp[tgt] == batch_sent[tgt]) //batch_sent[tgt] == -2 <=> end tag is already sent
			{//end_tag sent (ignore tgt) || nothing to send
				if(batch_sent[tgt] != -2) send_endTag(tgt); //nothing to send, but end tag not sent yet, so we send end tag
				//forward one "tgt"
				tgt++;
				if(tgt >= _num_workers) tgt -= _num_workers;
				//stop after a round, and start "tgt" still has nothing to send
				cnt++;
				if(cnt > _num_workers) return false;
			}
			cur_pos = tgt;
			return true;
    	}
    	else
    	{
    		//go to a "tgt" where new file is available for sending
			int tgt = cur_pos;
			int cnt = 0;
			while(sender_batch_comp[tgt] == batch_sent[tgt])
			{//end_tag sent (ignore tgt) || nothing to send
				//forward one "tgt"
				tgt++;
				if(tgt >= _num_workers) tgt -= _num_workers;
				//stop after a round, and start "tgt" still has nothing to send
				cnt++;
				if(cnt > _num_workers) return false;
			}
			cur_pos = tgt;
			return true;
    	}
    }

	bool wait_for_receiver() //return true if job terminates
	{
		//two notification cases:
		//1. when recv-er finish sync, and add rstep_after, recv-er will notify sender
		//2. when recv-er finish sync, and set job_finished to true, recv-er will notify sender
		unique_lock<mutex> lock_r2s(mtx_r2s);
		while(sstep > rstep_after && !job_finished) cond_r2s.wait(lock_r2s);
		return job_finished;
	}

	bool nothing_to_send()
	{//lock of numfiles_comped_step needs to be obtained outside
		if((int)numfiles_comped_step.size() - 1 < sstep) return true;
		return numfiles_comped_step[sstep] == numfiles_sent;
	}

	bool wait_for_computer() //return true if superstep terminates
	{
		//two notification cases:
		//1. when comp-er generates a new file, comp-er will notify sender
		//2. when comp-er finishes computation of all vertices, and add cstep, comp-er will notify sender
		unique_lock<mutex> lock_c2s(mtx_c2s);
		while(sstep == global_step_num && nothing_to_send()) cond_c2s.wait(lock_c2s); //if case 1 holds, numfiles_comped is no longer valid, but will not be checked anyway
		return sstep < global_step_num;
	}

    //interface to outside
	void run()
    {
		while(true)
		{
    		if(wait_for_receiver()) break; //notified by recv-er because job_finished
    		wait_for_batchComp(); //before scanning, make sure sender_batch_comp[sstep] is ready
    		while(true)
    		{
    			if(scan()) send();
    			else //no file to send
    			//{
    				if(wait_for_computer()) //no more files to send for current superstep
    				{
    					while(scan()) send();
    					delete[] sender_batch_comp;
    					clear();
    					if(_my_rank == MASTER_RANK) cout<<"- Sender done (for step "<<sstep<<")"<<endl;
    					sstep ++;
    					break; //go to outer-loop, wait to enter next superstep
    				}
    				//else, go to if-branch of inner-loop, to fetch file again
    			//}
    		}
		}
    }

private:
    int* batch_sent; //batch_sent[i] = last batch that gets sent to worker i
    int cur_pos; //current position in array of targets
    //---
    int sstep; //the superstep sender wants to compute, or are computing
    int numfiles_sent; //number of files sent in current superstep
    //--- assisting buffers
    char fname[100]; //buffer for obtaining file name
    char num[20]; //buffer for writing numbers
    //---
    atomic<int> * sender_batch_comp; //to differentiate from that of Comper (in global.h)
    //---
    CombinerT combiner;
    MessageT& zero;
    vector<MessageT> aggbuf;
};

//potential problem:
//- send() = read + send, serial disk IO + network IO, which can be parallelized to improve performance (prefetching to glob_sendbuf2, glob_sendbuf3 ...)
//- simplification note: here, since we will run multiple workers per server, parallelization is achieved due to multiple processes

#endif
