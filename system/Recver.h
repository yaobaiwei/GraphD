#ifndef RECVER_H
#define RECVER_H

#include "global.h"
#include "serialization.h"
#include "ioser.h"
#include "communication.h"
#include "Msg.h"
#include <mpi.h>
#include <string.h>
#include <fstream>
#include <condition_variable>
#include <mutex>
#include <unistd.h> //for usleep()
#include <queue>
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
class Recver
{
public:
	typedef vector<VertexT> VertexContainer;
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
	typedef typename VertexT::EdgeType EdgeT;
	typedef typename VertexT::MessageType MessageT;
	//----
	typedef IDMsg<VertexT> IDMsgT;

	Recver() : vertexes(*(VertexContainer *)global_vertices)
	{
		rstep_before = 0;
		rstep_after = 0;
		job_finished = false;
	}

	//set filename
	void set_fname(int msg_bat, int level = 0)
	{
		//get filename
		strcpy(fname, IOPREGEL_RECVER_DIR.c_str());
		sprintf(num, "/%d_", rstep_before.load(memory_order_relaxed));
		strcat(fname, num);
		sprintf(num, "%d_", _my_rank);
		strcat(fname, num);
		sprintf(num, "%d", msg_bat);
		strcat(fname, num);
		if(level > 0)
		{
			sprintf(num, "_%d", level);
			strcat(fname, num);
		}
	}

	struct EdgedVertex
	{
		VertexT v;
		vector<EdgeT> edges;

		//serialize into disk stream
		friend ofbinstream& operator<<(ofbinstream& m, const EdgedVertex& ev)
		{
			m << ev.v;
			int size = ev.v.degree;
			for(int i=0; i<ev.v.degree; i++) m << ev.edges[i];
			return m;
		}

		//serialize out from disk stream
		friend ifbinstream& operator>>(ifbinstream& m, EdgedVertex& ev)
		{
			m >> ev.v;
			int size = ev.v.degree;
			ev.edges.resize(size);
			for(int i=0; i<ev.v.degree; i++) m >> ev.edges[i];
			return m;
		}

		//serialize into in-mem stream
		friend obinstream& operator<<(obinstream& m, const EdgedVertex& ev)
		{
			m << ev.v;
			int size = ev.v.degree;
			for(int i=0; i<ev.v.degree; i++) m << ev.edges[i];
			return m;
		}

		//serialize out from in-mem stream
		friend ibinstream& operator>>(ibinstream& m, EdgedVertex& ev)
		{
			m >> ev.v;
			int size = ev.v.degree;
			ev.edges.resize(size);
			for(int i=0; i<ev.v.degree; i++) m >> ev.edges[i];
			return m;
		}

		inline bool operator<(const EdgedVertex& rhs) const
		{
			return v.id < rhs.v.id;
		}
	};

	//recv a batch
	void recv()
	{
	    MPI_Status status;
		MPI_Recv(glob_recvbuf, MAX_MSG_STREAM_SIZE, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status); //busy waiting if msg is not available
		int len;
		MPI_Get_count(&status, MPI_CHAR, &len);
		if(len > 0)
		{//Case 1: data-msg
			if(rstep_before == 0)
			{//iterations for graph loading
				//sort vertices before writing
				ibinstream um(glob_recvbuf, len); //create stream from memory buffer directly
				vector<EdgedVertex> edged_vertexes; //container for in-mem sort
				EdgedVertex v;
				while(!um.end())
				{
					edged_vertexes.push_back(v);
					um >> edged_vertexes.back();
				}
				sort(edged_vertexes.begin(), edged_vertexes.end());
				obinstream sorted; //sorted msg-stream
				for(int i=0; i<edged_vertexes.size(); i++) sorted << edged_vertexes[i];
				//get filename
				set_fname(batNum); //level 0 file for merge-sort
				batNum ++;
				//write to the file
				ofstream out(fname, ofstream::binary);
				out.write(sorted.get_buf(), sorted.size());
				out.close();
			}
			else
			{//iterations for computation
				if(global_combiner_used) //no need to sort, sorted by sender already
				{
					//get filename
					set_fname(batNum); //level 0 file for merge-sort
					batNum ++;
					//write to the file
					ofstream out(fname, ofstream::binary);
					out.write(glob_recvbuf, len);
					out.close();
				}
				else
				{
					//sort msgs before writing
					ibinstream um(glob_recvbuf, len); //create stream from memory buffer directly
					vector<IDMsgT> msgs; //container for in-mem sort
					IDMsgT msg;
					while(!um.end())
					{
						um >> msg;
						msgs.push_back(msg);
					}
					sort(msgs.begin(), msgs.end());
					obinstream sorted; //sorted msg-stream
					for(int i=0; i<msgs.size(); i++) sorted << msgs[i];
					//get filename
					set_fname(batNum); //level 0 file for merge-sort
					batNum ++;
					//write to the file
					ofstream out(fname, ofstream::binary);
					out.write(sorted.get_buf(), sorted.size());
					out.close();
				}
			}
		}
		else
		{//Case 2: len == 0, endTag
			rcount++;
		}
	}

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
		for(size_t i=0; i<in.size(); i++) //init: put first element of each in-stream to heap
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
		for(size_t i=0; i<in.size(); i++) in[i].close();
	}

	//entry interface for merge sort
	void msg_merge_sort(int num_files)
	{
		int level = 0; //current level
		int fnum = num_files; //number of files in current level
		while(fnum > 1)
		{
			//process full batches
			int bnum = fnum / NUM_WAY_OF_MERGE; //number of full batches
			for(int i=0; i<bnum; i++)
			{
                vector<ifbinstream> in(NUM_WAY_OF_MERGE);
                ofbinstream out;
				//open in-streams
				for(int j=0; j<NUM_WAY_OF_MERGE; j++)
				{
					set_fname(i * NUM_WAY_OF_MERGE + j, level);
					in[j].open(fname);
				}
				//open out-stream
				set_fname(i, level + 1);
				out.open(fname);
				//merge in-streams into out-stream
				msg_merge(in, out);
				//delete in-streams as they are consumed
				for(int j=0; j<NUM_WAY_OF_MERGE; j++)
				{
					set_fname(i * NUM_WAY_OF_MERGE + j, level);
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
					set_fname(bnum * NUM_WAY_OF_MERGE + j, level);
					in[j].open(fname);
				}
				//open out-stream
				set_fname(bnum, level + 1);
				out.open(fname);
				//merge in-streams into out-stream
				msg_merge(in, out);
				//delete in-streams as they are consumed
				for(int j=0; j<last_size; j++)
				{
					set_fname(bnum * NUM_WAY_OF_MERGE + j, level);
					remove(fname);
				}
				//update iterating variable "fnum"
				fnum = bnum + 1;
			}
			//update iterating variable "level"
			level++;
		}
		//now only the merged file is left, rename it as "M_stream_(_my_rank)_(rstep_before)"
		set_fname(0, level); //this is the old fname
		char new_name[1000];
		strcpy(new_name, IOPREGEL_RECVER_DIR.c_str());
		strcat(new_name, "/M_stream");
		sprintf(num, "_%d", _my_rank);
		strcat(new_name, num);
		sprintf(num, "_%d", rstep_before + 1); //!!! the "+1" is important, as the messages are towards next superstep
		strcat(new_name, num);
		rename(fname, new_name);
	}

	struct EVSID //IDMsgT expanded with stream ID, used by merge(.)
	{
		EdgedVertex ev;
		int sid;

		inline bool operator<(const EVSID& rhs) const //tgt-vid is the key
		{
			return ev.v.id > rhs.ev.v.id;  //reverse to ">" because priority_queue is a max-heap
		}
	};

	//sub-function of ev_merge_sort
	void ev_merge(vector<ifbinstream>& in, ofbinstream& out) //open outside, close inside
	{
		priority_queue<EVSID> pq;
		EVSID tmp;
		for(int i=0; i<in.size(); i++) //init: put first element of each in-stream to heap
		{
			in[i] >> tmp.ev;
			tmp.sid = i;
			pq.push(tmp);
		}
		while(!pq.empty())
		{
			tmp = pq.top(); //pick min
			pq.pop();
			out << tmp.ev; //append to out-stream
			ifbinstream& ins = in[tmp.sid];
			if(!ins.eof()) //get one more entry from the digested in-stream
			{
				ins >> tmp.ev;
				//tmp.sid remains the same as the popped entry
				pq.push(tmp);
			}
		}
		//---
		out.close();
		for(int i=0; i<in.size(); i++) in[i].close();
	}

	//entry interface for merge sort
	void ev_merge_sort(int num_files)
	{
		//create vertex file
		strcpy(fname, IOPREGEL_GRAPH_DIR.c_str());
		strcat(fname, "/V_stream");
		sprintf(num, "_%d", _my_rank);
		strcat(fname, num);
		ofbinstream vout(fname);
		//create edge file
		strcpy(fname, IOPREGEL_GRAPH_DIR.c_str());
		strcat(fname, "/E_stream");
		sprintf(num, "_%d", _my_rank);
		strcat(fname, num);
		ofbinstream eout(fname);
		//------------------------------------------
		int level = 0; //current level
		int fnum = num_files; //number of files in current level
		vector<ifbinstream> in;
		ofbinstream out;
		while(fnum > 1)
		{
			//process full batches
			int bnum = fnum / NUM_WAY_OF_MERGE; //number of full batches
			for(int i=0; i<bnum; i++)
			{
                vector<ifbinstream> in(NUM_WAY_OF_MERGE);
                ofbinstream out;
				//open in-streams
				for(int j=0; j<NUM_WAY_OF_MERGE; j++)
				{
					set_fname(i * NUM_WAY_OF_MERGE + j, level);
					in[j].open(fname);
				}
				//open out-stream
				set_fname(i, level + 1);
				out.open(fname);
				//merge in-streams into out-stream
				ev_merge(in, out);
				//delete in-streams as they are consumed
				for(int j=0; j<NUM_WAY_OF_MERGE; j++)
				{
					set_fname(i * NUM_WAY_OF_MERGE + j, level);
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
					set_fname(bnum * NUM_WAY_OF_MERGE + j, level);
					in[j].open(fname);
				}
				//open out-stream
				set_fname(bnum, level + 1);
				out.open(fname);
				//merge in-streams into out-stream
				ev_merge(in, out);
				//delete in-streams as they are consumed
				for(int j=0; j<last_size; j++)
				{
					set_fname(bnum * NUM_WAY_OF_MERGE + j, level);
					remove(fname);
				}
				//update iterating variable "fnum"
				fnum = bnum + 1;
			}
			//update iterating variable "level"
			level++;
		}
		//now split the merged file to V-stream and E-stream
		//------------------------------------------
		set_fname(0, level); //this is the old fname
		ifbinstream fin(fname); //input from the merged file
		EdgedVertex ev;
		EdgeT edge;
		//----
		active_vlock.lock();
		active_vnum_step.resize(1);
		active_vnum_step[0] = 0;
		active_vlock.unlock();
		//----
		while(!fin.eof())
		{
			//write to V-stream: <I><V><active><numNbs>, so that later, may load from local
			//- store numNbs with vertices, to facilitate potential random access of adjacency lists
			fin >> ev;
			VertexT& v = ev.v;
			vout << v;
			//append vertex to in-memory vertex array
			v.pos = vertexes.size(); //--- just for basic mode, to support vertex.recoded_id()
			vertexes.push_back(v);
			//write to E-stream: nb1 nb2 ...
			for(int i=0; i<v.degree; i++)
			{
				eout << ev.edges[i];
			}
			if(v.active){
				active_vlock.lock();
				active_vnum_step[0]++;
				active_vlock.unlock();
			}
		}
		remove(fname); //remove the sorted file
		//------------------------------------------
		fin.close();
		vout.close();
		eout.close();
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
			//------ post processing received msg batches
			if(rstep_before == 0 && global_hdfs_load) //create graph files in loading phase
			{
				//handling shuffled vertices
				if(batNum > 0)
				{
					ev_merge_sort(batNum); //sort (v, {e}) by vid, and split to v-stream and e-stream
				}
				else //the worker contains no vertices, but we still write empty files
				{
					//will need it for recver-sync
					active_vlock.lock();
					active_vnum_step.resize(1);
					active_vnum_step[0] = 0;
					active_vlock.unlock();
					//create vertex file
					strcpy(fname, IOPREGEL_GRAPH_DIR.c_str());
					strcat(fname, "/V_stream");
					sprintf(num, "_%d", _my_rank);
					strcat(fname, num);
					ofbinstream vout(fname);
					//create edge file
					strcpy(fname, IOPREGEL_GRAPH_DIR.c_str());
					strcat(fname, "/E_stream");
					sprintf(num, "_%d", _my_rank);
					strcat(fname, num);
					ofbinstream eout(fname);
					//------
					vout.close();
					eout.close();
				}
				//adjust buf size
				if(global_combiner_used)
				{
					//get max_i Vi
					global_max_vnum = all_max(vertexes.size());
					//2. resize comm-bufs to support msg-combining
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
			else //merge-sort msgs
			{
				if(batNum > 0) msg_merge_sort(batNum);//if batNum == 0, there's no message to process
				//after this call
				//- "M_stream_(_my_rank)_(rstep_before)" is written, ready to be used by Comper
				//- Intermediate files are guaranteed to be deleted
				//- ToDo: let Comper delete "M_stream_(_my_rank)_(rstep_before)" after the file is used
			}
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
				if(global_combiner_used)
				{
					int required_size = sizeof(IDMsgT) * global_max_vnum;
					if(required_size > MAX_MSG_STREAM_SIZE)
					{
						MAX_MSG_STREAM_SIZE = required_size;
						delete glob_sendbuf;
						delete glob_recvbuf;
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
    char fname[1000]; //buffer for obtaining file name
    char num[20]; //buffer for writing numbers
    //---
    VertexContainer& vertexes;
};

//potential problem:
//- recv() = recv + sort + write, network IO + CPU + serial disk IO, which can be parallelized to improve performance
//- simplification note: here, since we will run multiple workers per server, parallelization is achieved due to multiple processes

#endif
