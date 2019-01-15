#ifndef COMPER_H
#define COMPER_H

#include <vector>
#include <iostream>
#include <fstream>
#include <typeinfo>
#include <typeindex>
#include "global.h"
#include "ydhdfs.h"
#include "timer.h"
#include "ioser.h"
#include "communication.h"
#include "Msg.h"
#include "Aggregator.h"
using namespace std;

template <class VertexT, class AggregatorT = DummyAgg<VertexT> >
class Comper
{
public:
	typedef vector<VertexT> VertexContainer;
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::MessageType MessageT;
	typedef typename VertexT::EdgeType EdgeT;
	typedef typename VertexT::HashType HashT;
	typedef vector<MessageT> MessageContainer;
	typedef vector<EdgeT> EdgeContainer;
	//----
	typedef IDMsg<VertexT> IDMsgT;
	//----
	typedef typename AggregatorT::PartialType PartialT;
	typedef typename AggregatorT::FinalType FinalT;

    Comper()
	{
		global_step_num = 0;
		out_buf.resize(_num_workers);
		//----
		global_vertices = &vertexes;
		//---
		if(type_index(typeid(AggregatorT)) != type_index(typeid(DummyAgg<VertexT>)))
		{
			global_aggregator_used = true;//to enable non-base combiner
			global_agg = new FinalT;
		}
	}

    ~Comper()
    {
    	if(global_aggregator_used) delete (FinalT*)global_agg;
    }

    bool wait_for_receiver()
    {
        unique_lock<mutex> lock_r2c(mtx_r2c);
        while(global_step_num > rstep_before && !job_finished) cond_r2c.wait(lock_r2c);
        return job_finished;
    }

    /* //move to Msg.h
    void wake_up_sender_fileWritten()
    {
        mtx_c2s.lock();
        numfiles_comped_step[global_step_num] ++;
        mtx_c2s.unlock();
        cond_c2s.notify_one();
    }
    */

    void wake_up_sender_compDone()
    {
        mtx_c2s.lock();
        global_step_num ++;
        mtx_c2s.unlock();
        cond_c2s.notify_one();
    }

    void create_batch_comp()
	{
		//make sure batch_comp[global_step_num] is allocated
		int size_req = global_step_num + 1;
		mtx_batComp.lock(); //batch_comp_step is not thread-safe, and resize may reallocate space read by Sender
		if(batch_comp_step.size() < size_req) batch_comp_step.resize(size_req);
		batch_comp = batch_comp_step[global_step_num] = new int[_num_workers];
		for (int i = 0; i < _num_workers; i++) batch_comp[i] = -1;
		mtx_batComp.unlock();
		cond_batComp.notify_one(); //Sender may wait_for_batchComp(), otherwise, batch_comp[] is not ready for scan()
		//make sure numfiles_comped[global_step_num] is allocated
		mtx_c2s.lock(); //numfiles_comped_step is not thread-safe, and resize may reallocate space read by Sender
		if(numfiles_comped_step.size() < size_req) numfiles_comped_step.resize(size_req);
		numfiles_comped_step[global_step_num] = 0;
		mtx_c2s.unlock();
	}

    /* // following functions are moved to Msg.h
    void outbuf_flush() //called after an iteration to make sure all streams are flushed, and outbuf is ready for use by next iteration

    //lazy file creation at the beginning, so that no empty file will be written
    //no need for later files, since if we create a new file, we must write sth to it
    void append(int tgt) //append mem_bytes to out_buf
    */

    //UDF for line parsing
	virtual KeyT parseVertex(char* line, obinstream& file_stream) = 0; // should return vertex ID
	//users need to parse line to get
	//1. <I>, <V>, <active>
	//2. {<E>}, <E> can be an (ID, edge_value) pair, or simply ID
	//append to file_stream: <I>, <V>, <active> numOfNbs nb1 nb2 ...

    //load an HDFS file
    void load_graph(const char* inpath)
	{
		hdfsFS fs = getHdfsFS();
		hdfsFile in = getRHandle(inpath, fs);
		LineReader reader(fs, in);
		while (true) {
			reader.readLine();
			if (!reader.eof())
			{
				KeyT vid = parseVertex(reader.getLine(), mem_bytes); //in_mem_stream is set
				append(hash(vid)); //in_mem_stream is consumed
			}
			else
				break;
		}
		hdfsCloseFile(fs, in);
		hdfsDisconnect(fs);
	}

    //========================== msg & edge stream ==========================
    //if return true
    //- need to call msgIn.close() after used
    //if return false
    //- the msg file does not exist, there is no incoming message
    bool open_msg_stream()
    {
    	//set fname to be the shuffled msgs
    	strcpy(fname, IOPREGEL_RECVER_DIR.c_str());
		strcat(fname, "/M_stream");
		sprintf(num, "_%d", _my_rank);
		strcat(fname, num);
		sprintf(num, "_%d", global_step_num);
		strcat(fname, num);
		//---
		return msgIn.open(fname);
    }

    void open_edge_stream() //need to call edgeIn.close() after used
	{
		//set fname to be the shuffled msgs
    	strcpy(fname, IOPREGEL_GRAPH_DIR.c_str());
		strcat(fname, "/E_stream");
		sprintf(num, "_%d", _my_rank);
		strcat(fname, num);
		//---
		edgeIn.open(fname);
	}

    void get_nxtMsg() //sets "next_msg" and "has_nxtMsg" properly
    {
    	if(msgIn.eof()) has_nxtMsg = false;
    	else
    	{
    		has_nxtMsg = true;
    		msgIn >> next_msg;
    	}
    }

    void get_edges(int degree) //sets "edges" properly
	{
    	EdgeT tmp;
    	for(int i=0; i<degree; i++)
    	{
    		edgeIn >> tmp;
    		edges.push_back(tmp);
    	}
	}

    void get_msgs(int vid) //sets "msgs" properly
	{
    	while(true)
    	{
    		if(has_nxtMsg)
			{
				if(next_msg.id == vid)
				{
					msgs.push_back(next_msg.msg);
					get_nxtMsg();
				}
				else return;
			}
    		else return;
    	}
	}

    //################### Computing logic of a superstep ###################
#ifdef SKIP_MODE
    void vertex_computation()
	{
    	open_edge_stream();
    	bool msg_recved = open_msg_stream(); //*** note that here fname contains msg-file
    	//init active_count
    	int size_req = global_step_num + 1;
    	if(active_vnum_step.size() < size_req) active_vnum_step.resize(size_req);
    	active_vnum_step[global_step_num] = 0;
    	//---
    	int skip = 0; //# of edges to skip (due to inactive vertices)
    	//get num_of_bytes for EdgeT
    	EdgeT tmp;
    	mem_bytes << tmp; //!!! require EdgeT to be fix-sized type here (e.g., cannot be vector)
    	int ebytes = mem_bytes.size();
    	mem_bytes.clear();
    	//---
    	num_called = 0;
    	if(msg_recved)
    	{
    		get_nxtMsg(); //fill the first msg-item
			for (int i = 0; i < vertexes.size(); i++) {
				//fill "msgs" with vertexes[i]'s content
				get_msgs(vertexes[i].id);
				//do computation
				if(vertexes[i].active || msgs.size() > 0)
				{
					if(skip > 0){
						edgeIn.skip(skip * ebytes);
						skip = 0;
					}
					get_edges(vertexes[i].degree); //prepare edges
					num_called ++;
					vertexes[i].activate();
					vertexes[i].compute(msgs, edges);
					if(global_aggregator_used) aggregator.stepPartial(&vertexes[i], edges);
					if(vertexes[i].active) active_vnum_step[global_step_num] ++;
					//clear used "msgs" and "edges", to be used by next vertex
					edges.clear();
					msgs.clear();
				}
				else skip += vertexes[i].degree;
			}
			//---
			msgIn.close();
			edgeIn.close();
			//remove used msg-stream
			remove(fname); //*** note that here fname contains msg-file, written by open_msg_stream()
    	}
    	else
    	{
    		for (int i = 0; i < vertexes.size(); i++) {
				//do computation
				if(vertexes[i].active)
				{
					if(skip > 0){
						edgeIn.skip(skip * ebytes);
						skip = 0;
					}
					get_edges(vertexes[i].degree); //prepare edges
					num_called ++;
					vertexes[i].activate();
					vertexes[i].compute(msgs, edges); //msgs is empty here
					if(global_aggregator_used) aggregator.stepPartial(&vertexes[i], edges);
					if (vertexes[i].active) active_vnum_step[global_step_num] ++;
					//clear used "edges", to be used by next vertex
					edges.clear();
				}
				else skip += vertexes[i].degree;
			}
			//---
			edgeIn.close();
    	}
	}
#else
    void vertex_computation()
	{
		open_edge_stream();
		bool msg_recved = open_msg_stream(); //*** note that here fname contains msg-file
		//init active_count
		int size_req = global_step_num + 1;
		if(active_vnum_step.size() < size_req) active_vnum_step.resize(size_req);
		active_vnum_step[global_step_num] = 0;
		//---
		if(msg_recved)
		{
			get_nxtMsg(); //fill the first msg-item
			//---
			num_called = 0;
			for (int i = 0; i < vertexes.size(); i++) {
				//fill "msgs" and "edges" with vertexes[i]'s content
				//- currently, we do this even for inactive vertices, to be improved later
				get_msgs(vertexes[i].id);
				get_edges(vertexes[i].degree);
				//do computation
				if(vertexes[i].active || msgs.size() > 0)
				{
					num_called ++;
					vertexes[i].activate();
					vertexes[i].compute(msgs, edges);
					if(global_aggregator_used) aggregator.stepPartial(&vertexes[i], edges);
					if(vertexes[i].active) active_vnum_step[global_step_num] ++;
				}
				//clear used "msgs" and "edges", to be used by next vertex
				msgs.clear();
				edges.clear();
			}
			//---
			msgIn.close();
			edgeIn.close();
			//remove used msg-stream
			remove(fname); //*** note that here fname contains msg-file, written by open_msg_stream()
		}
		else
		{
			num_called = 0;
			for (int i = 0; i < vertexes.size(); i++) {
				//fill "edges" for vertexes[i]
				//- currently, we do this even for inactive vertices, to be improved later
				get_edges(vertexes[i].degree);
				//do computation
				if(vertexes[i].active)
				{
					num_called ++;
					vertexes[i].activate();
					vertexes[i].compute(msgs, edges); //msgs is empty here
					if(global_aggregator_used) aggregator.stepPartial(&vertexes[i], edges);
					if (vertexes[i].active) active_vnum_step[global_step_num] ++;
				}
				//clear used "edges", to be used by next vertex
				edges.clear();
			}
			//---
			edgeIn.close();
		}
	}
#endif

    //UDF for result dumping, default implementation does nothing
    //functions for local dump
    virtual void to_line(VertexT& v, vector<EdgeT>& edges, ofstream& fout){} //this one needs to read edge-stream
    virtual void to_line(VertexT& v, ofstream& fout){} //this one only access v-state from main memory
    //functions for hdfs dump
    virtual void to_line(VertexT& v, vector<EdgeT>& edges, BufferedWriter& fout){} //this one needs to read edge-stream
    virtual void to_line(VertexT& v, BufferedWriter& fout){} //this one only access v-state from main memory

    //functions for pre-step and post-step processing, do nothing by default
	virtual void step_begin(){}
	virtual void step_end(){}

    void hdfs_load(const WorkerParams& params)
    {
    	ResetTimer(WORKER_TIMER);
		//check IO path
		if (_my_rank == MASTER_RANK) {
			if (dirCheck(params.input_path.c_str()) == -1) MPI_Abort(MPI_COMM_WORLD, 1);
			if(params.hdfs_dump) if (dirCheck(params.output_path.c_str(), params.force_write) == -1) MPI_Abort(MPI_COMM_WORLD, 1);
		}
		init_timers();
		//----
		create_batch_comp(); //init data structures of Comper for sync with Sender

		//dispatch splits
		vector<vector<string> >* arrangement;
		if (_my_rank == MASTER_RANK) {
			arrangement = dispatchRan(params.input_path.c_str());
			masterScatter(*arrangement);
			vector<string>& assignedSplits = (*arrangement)[0];
			//reading assigned splits
			for(int i=0; i<assignedSplits.size(); i++) load_graph(assignedSplits[i].c_str());
			delete arrangement;
		} else {
			vector<string> assignedSplits;
			slaveScatter(assignedSplits);
			//reading assigned splits
			for(int i=0; i<assignedSplits.size(); i++) load_graph(assignedSplits[i].c_str());
		}

		//----
		outbuf_flush(); //make sure all files are sync-ed to disk
		wake_up_sender_compDone(); //must be called at the end of each round of Comper, to allow sender to send end tags
		//----
		StopTimer(WORKER_TIMER);
		PrintTimer("HDFS Load Time", WORKER_TIMER); //may not be accurate, only reflects time of worker 0
    }

    void local_load(const WorkerParams& params) //input_path is fixed to IOPREGEL_GRAPH_DIR in this version
	{
		ResetTimer(WORKER_TIMER);
		//check IO path
		if (_my_rank == MASTER_RANK) {
			if(params.hdfs_dump) if (dirCheck(params.output_path.c_str(), params.force_write) == -1) MPI_Abort(MPI_COMM_WORLD, 1);
		}
		init_timers();
		//----
		create_batch_comp(); //init data structures of Comper for sync with Sender
		active_vnum_step.resize(1);
		active_vnum_step[0] = 0;
		//----
		//load V-stream into v-array
		strcpy(fname, IOPREGEL_GRAPH_DIR.c_str());
		strcat(fname, "/V_stream");
		sprintf(num, "_%d", _my_rank);
		strcat(fname, num);
		ifbinstream in(fname);
		VertexT tmp;
		while(!in.eof())
		{
			in >> tmp;
			tmp.pos = vertexes.size();
			vertexes.push_back(tmp);
			if(tmp.active) active_vnum_step[0]++;
		}
		in.close();
		//---- adjust buf size
		if(global_combiner_used)
		{
			//1. get Vi for all i
			global_vsize = new int[_num_workers];
			gather_vsizes(vertexes.size());
			//2. resize comm-bufs to support msg-combining
			int max = -1;
			for(int i=0; i<_num_workers; i++)
				if(global_vsize[i] > max) max = global_vsize[i];
			global_max_vnum = max; // !!! record max to global-structure
			/* move to recver
			int required_size = sizeof(IDMsgT) * max;
			if(required_size > MAX_MSG_STREAM_SIZE)
			{
				MAX_MSG_STREAM_SIZE = required_size;
				delete glob_sendbuf;
				delete glob_recvbuf;
				glob_sendbuf = new char[required_size];
				glob_recvbuf = new char[required_size];
			}
			*/
		}
		//----
		wake_up_sender_compDone(); //must be called at the end of each round of Comper, to allow sender to send end tags
		//----
		StopTimer(WORKER_TIMER);
		PrintTimer("Local Load Time", WORKER_TIMER); //may not be accurate, only reflects time of worker 0
	}

    void local_dump(const WorkerParams& params)
    {
    	IOPREGEL_RESULT_DIR = params.output_path;
		_mkdir(IOPREGEL_RESULT_DIR.c_str());
		ResetTimer(WORKER_TIMER);
		strcpy(fname, IOPREGEL_RESULT_DIR.c_str());
		sprintf(num, "/result_%d", _my_rank);
		strcat(fname, num);
		ofstream fout(fname);
		if(params.dump_with_edges)
		{
			open_edge_stream();
			for(int i=0; i<vertexes.size(); i++)
			{
				get_edges(vertexes[i].degree);
				to_line(vertexes[i], edges, fout);
				edges.clear();
			}
			edgeIn.close();
		}
		else
		{
			for(int i=0; i<vertexes.size(); i++) to_line(vertexes[i], fout);
		}
		fout.close();
		StopTimer(WORKER_TIMER);
		PrintTimer("Local Dump Time", WORKER_TIMER); //may not be accurate, only reflects time of worker 0
    }

    void hdfs_dump(const WorkerParams& params)
	{
    	ResetTimer(WORKER_TIMER);
    	hdfsFS fs = getHdfsFS();
    	sprintf(num, "/result_%d", _my_rank);
    	strcat(fname, num);
		BufferedWriter* writer = new BufferedWriter((params.output_path + num).c_str(), fs);
		if(params.dump_with_edges)
		{
			open_edge_stream();
			for(int i=0; i<vertexes.size(); i++)
			{
				get_edges(vertexes[i].degree);
				writer->check();
				to_line(vertexes[i], edges, *writer);
				edges.clear();
			}
			edgeIn.close();
		}
		else
		{
			for(int i=0; i<vertexes.size(); i++)
			{
				writer->check();
				to_line(vertexes[i], *writer);
			}
		}
		delete writer;
		hdfsDisconnect(fs);
		StopTimer(WORKER_TIMER);
		PrintTimer("HDFS Dump Time", WORKER_TIMER); //may not be accurate, only reflects time of worker 0
	}

    //################### aggregator logic of a superstep ###################
    /* //MPICH-3.1.3 seems to have bugs, and MPI_Bcast cannot work properly, replaced by sending through COMM_CHANNEL_100
    void agg_sync()
	{
    	if (_my_rank != MASTER_RANK) { //send partialT to aggregator
			//gathering PartialT
			PartialT* part = aggregator.finishPartial();
			slaveGather(*part);
			//scattering FinalT
			slaveBcast(*((FinalT*)global_agg));
		} else {
			//gathering PartialT
			vector<PartialT*> parts(_num_workers);
			masterGather(parts);
			for (int i = 0; i < _num_workers; i++) {
				if (i != MASTER_RANK) {
					PartialT* part = parts[i];
					aggregator.stepFinal(part);
					delete part;
				}
			}
			//scattering FinalT
			FinalT* final = aggregator.finishFinal();
			//cannot set "global_agg=final" since MASTER_RANK works as a slave, and agg->finishFinal() may change
			*((FinalT*)global_agg) = *final; //deep copy
			masterBcast(*((FinalT*)global_agg));
		}
	}
	*/

    void agg_sync() //implementation to avoid bug of MPICH-3.1.3 MPI_Bcast & MPI_Gather, send through COMM_CHANNEL_100 (see communication.h)
	{
		if (_my_rank != MASTER_RANK) { //send partialT to aggregator
			//gathering PartialT
			PartialT* part = aggregator.finishPartial();
			send_data(*part, MASTER_RANK);
			//scattering FinalT
			*((FinalT*)global_agg) = recv_data<FinalT>(MASTER_RANK);
		} else {
			//gathering PartialT
            for (int i = 0; i < _num_workers; i++)//all but self
            {
                if(i == _my_rank) continue;
                PartialT part = recv_data<PartialT>(i);
                aggregator.stepFinal(&part);
            }
			//scattering FinalT
			FinalT* final = aggregator.finishFinal();
			//cannot set "global_agg=final" since MASTER_RANK works as a slave, and agg->finishFinal() may change
			*((FinalT*)global_agg) = *final; //deep copy
			for(int i=0; i<_num_workers; i++)
			{
				if(i != MASTER_RANK) send_data(*final, i);
			}
		}
	}

    void run(const WorkerParams& params)
	{
    	//################### Load BEGIN ###################
    	if(global_hdfs_load) hdfs_load(params);
    	else local_load(params); //please make sure -np is the same as the previous run with hdfs_load(.)
		//################### Load END ###################

		//################### Compute BEGIN ###################
    	while(true)
		{
			ResetTimer(WAIT_TIMER);
			//----
			if (wait_for_receiver()){
				if(global_step_num > rstep_before) break;
				//else{ force Comper to run the dummy superstep, so that agg_sync() is guaranteed to be called on everyone at step (last + 1) }
			}
			//report time
			StopTimer(WAIT_TIMER);
			PrintTimer("Comper_Wait Time", WAIT_TIMER); //may not be accurate, only reflects time of worker 0
			//---
			if(global_step_num > 1)
			{
				StopTimer(WORKER_TIMER);
				string report("*** *** *** *** *** *** Superstep ");
				sprintf(num, "%d", global_step_num - 1); //communication of last superstep ends, and wake_up_sender_compDone() already incremented global_step_num
				report += num; //use the written step-num
				report += " Total Time";
				PrintTimer(report.c_str(), WORKER_TIMER); //may not be accurate, only reflects time of worker 0
			}
			//----
			ResetTimer(WORKER_TIMER);
			ResetTimer(COMPUTE_TIMER);
			create_batch_comp();
			//Computing a superstep
			if(global_aggregator_used) aggregator.init();
			clearBits(); //init end-condition bitmap
			step_begin(); //--- one may open a file stream here					
			vertex_computation(); //--- one may write to the file stream in compute()
			//----
			outbuf_flush();//make sure all files are sync-ed to disk
			if(global_aggregator_used) agg_sync();
			step_end(); //--- one may close a file stream here
			sprintf(num, "%d", global_step_num); //need to get step-num here, since wake_up_sender_compDone() increments step-num
			wake_up_sender_compDone();								   
			//report time
			StopTimer(COMPUTE_TIMER);
			PrintTimer("Computation Time", COMPUTE_TIMER); //may not be accurate, only reflects time of worker 0
		}
		//################### Compute END ###################

		if(params.dump_disabled == false)
		{
			//################### Dump BEGIN ###################
			if(params.hdfs_dump) hdfs_dump(params);
			else local_dump(params); //please make sure -np is the same as the previous run with hdfs_load(.)
			//################### Dump END ###################
		}
	}

private:
    char fname[1000];
    char num[20];
    //---
    HashT hash;
    VertexContainer vertexes;
    //---
    int num_called;
    //--- tmp-structures for holding input to compute()
    MessageContainer msgs;
    EdgeContainer edges;
    //--- structures for iterating msg & edge streams
    ifbinstream msgIn;
    ifbinstream edgeIn;
    IDMsgT next_msg;
    bool has_nxtMsg;
    //---
    AggregatorT aggregator; //only required by Comper
};

#endif

