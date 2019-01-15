#ifndef MSG_H
#define MSG_H

#include "ioser.h"
#include "serialization.h"
#include "global.h"
#include <atomic>

template <class VertexT>
class IDMsg
{
public:
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::MessageType MessageT;
	typedef IDMsg<VertexT> IDMsgT;

	KeyT id;
	MessageT msg;

	IDMsg(){}

	IDMsg(KeyT key, MessageT message)
	{
		id = key;
		msg = message;
	}

	//serialize into disk stream
	friend ofbinstream& operator<<(ofbinstream& m, const IDMsgT& v)
	{
		m << v.id;
		m << v.msg;
		return m;
	}

	//serialize out from disk stream
	friend ifbinstream& operator>>(ifbinstream& m, IDMsgT& v)
	{
		m >> v.id;
		m >> v.msg;
		return m;
	}

	//serialize into in-mem stream
	friend obinstream& operator<<(obinstream& m, const IDMsgT& v)
	{
		m << v.id;
		m << v.msg;
		return m;
	}

	//serialize out from in-mem stream
	friend ibinstream& operator>>(ibinstream& m, IDMsgT& v)
	{
		m >> v.id;
		m >> v.msg;
		return m;
	}

	inline bool operator<(const IDMsgT& rhs) const
	{
		return id < rhs.id;
	}

	inline bool operator==(const IDMsgT& rhs) const
	{
		return id == rhs.id;
	}

	inline bool operator!=(const IDMsgT& rhs) const
	{
		return id != rhs.id;
	}
};

//#########################################
//Comper-thread's functions to send msgs (shared by Comper.h and Vertex.h)
atomic<int> * batch_comp;
//--- _num_workers (ofbinstream)s for vertex/msg appending
vector<ofbinstream> out_buf;
obinstream mem_bytes;
//---
void wake_up_sender_fileWritten()
{
	mtx_c2s.lock();
	numfiles_comped_step[global_step_num] ++;
	mtx_c2s.unlock();
	cond_c2s.notify_one();
}
//---
void outbuf_flush() //called after an iteration to make sure all streams are flushed, and outbuf is ready for use by next iteration
{
	for(int i=0; i<_num_workers; i++)
		if(out_buf[i].is_open())
		{
			out_buf[i].close();
			batch_comp[i]++; //forward after file is written, so sender may send the file
		}
	wake_up_sender_fileWritten(); //notify sender to send written file(s)
}
//---
//lazy file creation at the beginning, so that no empty file will be written
//no need for later files, since if we create a new file, we must write sth to it
void append(int tgt) //append mem_bytes to out_buf
{
	ofbinstream& fout = out_buf[tgt];
	bool to_create = false;
	if(!fout.is_open()) to_create = true; //no file is written
	else if(fout.size() + mem_bytes.size() > MAX_SPLIT_SIZE) //file will overflow
	{
		fout.close();
		batch_comp[tgt]++; //forward only after file is written
		wake_up_sender_fileWritten(); //notify sender to send written file
		to_create = true;
	}
	//----
	if(to_create)
	{
		char fname[100];
		char num[20];
		strcpy(fname, IOPREGEL_SENDER_DIR.c_str());
		sprintf(num, "/%d_", global_step_num.load(memory_order_relaxed));
		strcat(fname, num);
		sprintf(num, "%d_", _my_rank);
		strcat(fname, num);
		sprintf(num, "%d_", tgt);
		strcat(fname, num);
		sprintf(num, "%d", batch_comp[tgt] + 1);
		strcat(fname, num);
		fout.open(fname);
	}
	fout << mem_bytes;
	mem_bytes.clear(); //mem_bytes is cleared automatically at last, no need to clear outside
}

#endif
