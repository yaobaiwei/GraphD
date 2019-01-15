#ifndef RECODECOMPER_H
#define RECODECOMPER_H

#include <vector>
#include <iostream>
#include <fstream>
#include <typeinfo>
#include <typeindex>
#include "global.h"
#include "ydhdfs.h"
#include "timer.h"
#include "communication.h"
#include "Msg.h"
#include "Aggregator.h"
using namespace std;

template <class VertexT, class AggregatorT = DummyAgg<VertexT> >
class RecodeComper
{
public:
	typedef vector<VertexT> VertexContainer;
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
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

	//UDF: to specify the identity element
	virtual MessageT get_zero() = 0;

	MessageT zero; //!!! must be set after constructor

	RecodeComper()
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

    ~RecodeComper()
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
		size_t size_req = global_step_num + 1;
		mtx_batComp.lock(); //batch_comp_step is not thread-safe, and resize may reallocate space read by Sender
		if(batch_comp_step.size() < size_req) batch_comp_step.resize(size_req);
		batch_comp_step[global_step_num] = batch_comp = new atomic<int>[_num_workers];
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

    //========================== edge stream ==========================
    void open_edge_stream() //need to call edgeIn.close() after used
	{
		//set fname to be the shuffled msgs
    	strcpy(fname, IOPREGEL_GRAPH_DIR.c_str());
		strcat(fname, "/E_recoded"); //read recoded stream
		sprintf(num, "_%d", _my_rank);
		strcat(fname, num);
		//---
		edgeIn.open(fname);
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

    //################### Computing logic of a superstep ###################
#ifdef SKIP_MODE
    void vertex_computation()
	{
		if(global_step_num > 1 && global_aggedVec_step.size() < global_step_num + 1) //the last (dummy) superstep that does nothing
		{//it should not fetch global_aggedVec_step[global_step_num], as recver will not create it
			return;
		}
		//------
		size_t skip = 0; //# of edges to skip (due to inactive vertices)
		//get num_of_bytes for EdgeT
		EdgeT tmp;
		mem_bytes << tmp; //!!! require EdgeT to be fix-sized type here (e.g., cannot be vector)
		size_t ebytes = mem_bytes.size();
		mem_bytes.clear();
		//init active_count
		size_t size_req = global_step_num + 1;
		active_vlock.lock();
		if(active_vnum_step.size() < size_req) active_vnum_step.resize(size_req);
		active_vnum_step[global_step_num] = 0;
		active_vlock.unlock();
		//------
		open_edge_stream();
		if(global_step_num == 1) //there is no message, and thus no "aggs"
		{
			for (size_t i = 0; i < vertexes.size(); i++) {
				//do computation
				if(vertexes[i].active)
				{
					if(skip > 0){
						edgeIn.skip(skip * ebytes);
						skip = 0;
					}
					get_edges(vertexes[i].degree); //prepare edges
					vertexes[i].activate();
					vertexes[i].compute(zero, edges);
					if(global_aggregator_used) aggregator.stepPartial(&vertexes[i], edges);
					if(vertexes[i].active){
						active_vlock.lock();
						active_vnum_step[global_step_num] ++;
						active_vlock.unlock();
					}
					//clear used "edges", to be used by next vertex
					edges.clear();
				}
				else skip += vertexes[i].degree;
			}
		}
		else
		{
			aggs = (MessageContainer *)global_aggedVec_step[global_step_num]; //open msg-vec
			MessageContainer& valueVec = * aggs;
			for (size_t i = 0; i < vertexes.size(); i++) {
				MessageT& agg_msg = valueVec[i]; //agg-ed msg
				//do computation
				if(vertexes[i].active || agg_msg != zero)
				{
					if(skip > 0){
						edgeIn.skip(skip * ebytes);
						skip = 0;
					}
					get_edges(vertexes[i].degree); //prepare edges
					vertexes[i].activate();
					vertexes[i].compute(agg_msg, edges);
					if(global_aggregator_used) aggregator.stepPartial(&vertexes[i], edges);
					if(vertexes[i].active){
						active_vlock.lock();
						active_vnum_step[global_step_num] ++;
						active_vlock.unlock();
					}
					//clear used "edges", to be used by next vertex
					edges.clear();
				}
				else skip += vertexes[i].degree;
			}
			//---
			delete aggs;//free used msg-vec
		}
		//---
		edgeIn.close();
	}
#else
    void vertex_computation()
	{
    	if(global_step_num > 1 && global_aggedVec_step.size() < global_step_num + 1) //the last (dummy) superstep that does nothing
    	{//it should not fetch global_aggedVec_step[global_step_num], as recver will not create it
    		return;
    	}
    	//------
    	open_edge_stream();
    	if(global_step_num == 1) //there is no message, and thus no "aggs"
    	{
    		//init active_count
    		size_t size_req = global_step_num + 1;
			active_vlock.lock();
			if(active_vnum_step.size() < size_req) active_vnum_step.resize(size_req);
			active_vnum_step[global_step_num] = 0;
			active_vlock.unlock();
			//---
			for (size_t i = 0; i < vertexes.size(); i++) {
				//fill "edges" for vertexes[i]
				//- currently, we do this even for inactive vertices, to be improved later
				get_edges(vertexes[i].degree);
				//do computation
				if(vertexes[i].active)
				{
					vertexes[i].activate();
					vertexes[i].compute(zero, edges);
					if(global_aggregator_used) aggregator.stepPartial(&vertexes[i], edges);
					if(vertexes[i].active){
						active_vlock.lock();
						active_vnum_step[global_step_num] ++;
						active_vlock.unlock();
					}
				}
				//clear used "edges", to be used by next vertex
				edges.clear();
			}
    	}
    	else
    	{
    		aggs = (MessageContainer *)global_aggedVec_step[global_step_num]; //open msg-vec
			MessageContainer& valueVec = * aggs;
			//---
			//init active_count
			size_t size_req = global_step_num + 1;
			active_vlock.lock();
			if(active_vnum_step.size() < size_req) active_vnum_step.resize(size_req);
			active_vnum_step[global_step_num] = 0;
			active_vlock.unlock();
			//---
			for (size_t i = 0; i < vertexes.size(); i++) {
				//fill "edges" for vertexes[i]
				//- currently, we do this even for inactive vertices, to be improved later
				get_edges(vertexes[i].degree);
				MessageT& agg_msg = valueVec[i]; //agg-ed msg
				//do computation
				if(vertexes[i].active || agg_msg != zero)
				{
					vertexes[i].activate();
					vertexes[i].compute(agg_msg, edges);
					if(global_aggregator_used) aggregator.stepPartial(&vertexes[i], edges);
					if(vertexes[i].active){
						active_vlock.lock();
						active_vnum_step[global_step_num] ++;
						active_vlock.unlock();
					}
				}
				//clear used "edges", to be used by next vertex
				edges.clear();
			}
			//---
			delete aggs;//free used msg-vec
    	}
		//---
		edgeIn.close();
	}
#endif

    //UDF for result dumping, default implementation does nothing
    //functions for local dump
    virtual void to_line(VertexT& v, vector<EdgeT>& edges, ofstream& fout){} //this one needs to read edge-stream
    virtual void to_line(VertexT& v, ofstream& fout){} //this one only access v-state from main memory
    //functions for hdfs dump
    virtual void to_line(VertexT& v, vector<EdgeT>& edges, BufferedWriter& fout){} //this one needs to read edge-stream
    virtual void to_line(VertexT& v, BufferedWriter& fout){} //this one only access v-state from main memory

    void local_load(const WorkerParams& params) //input_path is fixed to IOPREGEL_GRAPH_DIR in this version
	{
		ResetTimer(WORKER_TIMER);
		//check IO path
		if (_my_rank == MASTER_RANK) {
			if(params.dump_disabled == false)
                if(params.hdfs_dump) if (dirCheck(params.output_path.c_str(), params.force_write) == -1) MPI_Abort(MPI_COMM_WORLD, 1);
		}
		init_timers();
		//----
		create_batch_comp(); //init data structures of Comper for sync with Sender
		active_vlock.lock();
		active_vnum_step.resize(1);
		active_vnum_step[0] = 0;
		active_vlock.unlock();
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
			tmp.id = _num_workers * vertexes.size() + _my_rank; //recoding pos to VID
			vertexes.push_back(tmp);
			if(tmp.active){
				active_vlock.lock();
				active_vnum_step[0]++;
				active_vlock.unlock();
			}
		}
		in.close();
		//---- adjust buf size
		if(senderside_combiner_used)
		{
			//get max_i Vi
			global_max_vnum = all_max(vertexes.size());
			/* move to recver
			size_t required_size = sizeof(IDMsgT) * max;
			if(required_size > MAX_MSG_STREAM_SIZE)
			{
				MAX_MSG_STREAM_SIZE = required_size;
				delete[] glob_sendbuf;
				delete[] glob_recvbuf;
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
			for(size_t i=0; i<vertexes.size(); i++)
			{
				get_edges(vertexes[i].degree);
				to_line(vertexes[i], edges, fout);
			}
			edgeIn.close();
		}
		else
		{
			for(size_t i=0; i<vertexes.size(); i++) to_line(vertexes[i], fout);
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
			for(size_t i=0; i<vertexes.size(); i++)
			{
				get_edges(vertexes[i].degree);
				writer->check();
				to_line(vertexes[i], edges, *writer);
			}
			edgeIn.close();
		}
		else
		{
			for(size_t i=0; i<vertexes.size(); i++)
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
    	local_load(params); //recoded-mode only read from local_root
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
			vertex_computation(); //--- one may write to the file stream in compute()
			//----
			outbuf_flush();//make sure all files are sync-ed to disk
			if(global_aggregator_used) agg_sync();
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
    char fname[100];
    char num[20];
    //---
    HashT hash;
    VertexContainer vertexes;
    MessageContainer * aggs;
    //--- tmp-structures for holding input to compute()
    EdgeContainer edges;
    //--- structures for iterating recoded edge stream
    ifbinstream edgeIn;
    //---
    //structures only required by Comper
    AggregatorT aggregator;
};

#endif
