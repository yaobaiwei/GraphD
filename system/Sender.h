#ifndef SENDER_H
#define SENDER_H

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
#include <queue>
using namespace std;

template <class VertexT>
class Sender {
public:
	typedef typename VertexT::KeyType KeyT;
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

	Sender()
	{
		cur_pos = _my_rank; //so that it is different for every one
		batch_sent = new int[_num_workers];
		sstep = 0;
		clear();
		//---
		if(type_index(typeid(CombinerT)) != type_index(typeid(Combiner<MessageT>))) global_combiner_used = true;//to enable non-base combiner
	}

	~Sender()
	{
		delete[] batch_sent;
	}

    //sending logic
    //- scanning all target channels
    //- if one has a subsequent batch on disk, !!! and not written by computer !!!, send the batch
    //- we scan through instead of continue sending batches for the same target worker, to even out the communication among machines

	//================================= to be used by combine_and_send_batches()
	struct MsgSID //IDMsgT expanded with stream ID, used by merge(.)
	{
		IDMsgT msg;
		int sid;

		inline bool operator<(const MsgSID& rhs) const //tgt-vid is the key
		{
			return msg.id > rhs.msg.id; //reverse to ">" because priority_queue is a max-heap
		}
	};

	//sub-function of msg_merge_sort
	void msg_merge(vector<ifbinstream>& in, ofbinstream& out) //open outside, close inside
	{
		priority_queue<MsgSID> pq;
		MsgSID tmp;
		for(int i=0; i<in.size(); i++) //init: put first element of each in-stream to heap
		{
			in[i] >> tmp.msg;
			tmp.sid = i;
			pq.push(tmp);
		}
		while(!pq.empty())
		{
			tmp = pq.top(); //pick min
			pq.pop();
			out << tmp.msg; //append to out-stream
			ifbinstream& ins = in[tmp.sid];
			if(!ins.eof()) //get one more entry from the digested in-stream
			{
				ins >> tmp.msg;
				//tmp.sid remains the same as the popped entry
				pq.push(tmp);
			}
		}
		//---
		out.close();
		for(int i=0; i<in.size(); i++) in[i].close();
	}

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

	//set filename for a computed batch
	void set_fname_merge(int msg_bat, int level)
	{
		strcpy(fname, IOPREGEL_SENDER_DIR.c_str());
		sprintf(num, "/m%d_", sstep); //files to merge start with "m"
		strcat(fname, num);
		sprintf(num, "%d_", _my_rank);
		strcat(fname, num);
		sprintf(num, "%d_", level);
		strcat(fname, num);
		sprintf(num, "%d", msg_bat);
		strcat(fname, num);
	}

	//entry interface for merge sort
	void merge_sort(int tgt, int batch1, int batch2) //merge msgs towards tgt in batches [batch1, batch2]
	{
		int level = 0; //current level
		int fnum = batch2 - batch1 + 1; //number of files in current level
		//sort level-0 files one by one
		for(int i=0; i<fnum; i++)
		{
			vector<IDMsgT> msgs;
			IDMsgT msg;
			//---
			set_fname_computed(batch1 + i);//old name of a batch to process
			ifbinstream in(fname);
			while(!in.eof())
			{
				in >> msg;
				msgs.push_back(msg);
			}
			in.close();
			remove(fname);
			//---
			sort(msgs.begin(), msgs.end());
			//---
			set_fname_merge(i, 0); //new file name for level 0
			ofbinstream out(fname);
			for(int j=0; j<msgs.size(); j++) out << msgs[j];
			out.close();
		}
		//---
		while(fnum > 1)
		{
            vector<ifbinstream> in(NUM_WAY_OF_MERGE);
            ofbinstream out;
			//process full batches
			int bnum = fnum / NUM_WAY_OF_MERGE; //number of full batches
			for(int i=0; i<bnum; i++)
			{
				//open in-streams
				for(int j=0; j<NUM_WAY_OF_MERGE; j++)
				{
					set_fname_merge(i * NUM_WAY_OF_MERGE + j, level);
					in[j].open(fname);
				}
				//open out-stream
				set_fname_merge(i, level + 1);
				out.open(fname);
				//merge in-streams into out-stream
				msg_merge(in, out);
				//delete in-streams as they are consumed
				for(int j=0; j<NUM_WAY_OF_MERGE; j++)
				{
					set_fname_merge(i * NUM_WAY_OF_MERGE + j, level);
					remove(fname);
				}
			}
			//process non-full last batch
			int last_size = fnum % NUM_WAY_OF_MERGE;
			if(last_size == 0)
			{//no non-full last batch
				//update iterating variable "fnum"
				fnum = bnum;
			}
			else
			{
                vector<ifbinstream> in(last_size);
                ofbinstream out;
				//open in-streams
				for(int j=0; j<last_size; j++)
				{
					set_fname_merge(bnum * NUM_WAY_OF_MERGE + j, level);
					in[j].open(fname);
				}
				//open out-stream
				set_fname_merge(bnum, level + 1);
				out.open(fname);
				//merge in-streams into out-stream
				msg_merge(in, out);
				//delete in-streams as they are consumed
				for(int j=0; j<last_size; j++)
				{
					set_fname_merge(bnum * NUM_WAY_OF_MERGE + j, level);
					remove(fname);
				}
				//update iterating variable "fnum"
				fnum = bnum + 1;
			}
			//update iterating variable "level"
			level++;
		}
		//now only the merged file is left, rename it as "to_combine_(_my_rank)"
		char new_name[1000];
		set_fname_merge(0, level); //this is the old fname
		strcpy(new_name, IOPREGEL_SENDER_DIR.c_str());
		strcat(new_name, "/to_combine");
		sprintf(num, "_%d", _my_rank);
		strcat(new_name, num);
		rename(fname, new_name);
	}

	//combine sorted msgs, serialize to "combined"
	void combine_sorted(obinstream& combined) //process file "to_combine_(_my_rank)"
	{
		char ifname[1000];
		strcpy(ifname, IOPREGEL_SENDER_DIR.c_str());
		strcat(ifname, "/to_combine");
		sprintf(num, "_%d", _my_rank);
		strcat(ifname, num);
		ifbinstream in(ifname);
		//---
		bool first = true;
		IDMsgT prev, cur;
		while(!in.eof())
		{
			in >> cur;
			if(first)
			{
				prev = cur;
				first = false;
			}
			else
			{
				if(cur.id == prev.id) combiner.combine(prev.msg, cur.msg);
				else
				{
					combined << prev; //flush
					prev = cur;
				}
			}
		}
		combined << prev; //final flush
		in.close();
		remove(ifname);
	}
	//==========================================================================

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
	{//send file "step_src_tgt_{batches}"
    	int batch1 = batch_sent[cur_pos] + 1;
    	int batch2 = sender_batch_comp[cur_pos];
    	//sort
    	merge_sort(cur_pos, batch1, batch2);
    	//combine
    	obinstream combined;
    	combine_sorted(combined); //sorted file is deleted here
    	//send
    	MPI_Send(combined.get_buf(), combined.size(), MPI_CHAR, cur_pos, 0, MPI_COMM_WORLD);
    	//update
    	numfiles_sent += batch2 - batch_sent[cur_pos];
    	batch_sent[cur_pos] = batch2;
	}

    void send()
	{
		if(global_combiner_used && sstep > 0) combine_and_send_batches(); //should not call during loading phase
		else send_a_batch();
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
    char fname[1000]; //buffer for obtaining file name
    char num[20]; //buffer for writing numbers
    //---
    atomic<int> * sender_batch_comp; //to differentiate from that of Comper (in global.h)
    //---
    CombinerT combiner;
};

//potential problem:
//- send() = read + send, serial disk IO + network IO, which can be parallelized to improve performance (prefetching to glob_sendbuf2, glob_sendbuf3 ...)
//- simplification note: here, since we will run multiple workers per server, parallelization is achieved due to multiple processes

#endif
