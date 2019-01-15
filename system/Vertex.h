#ifndef VERTEX_H
#define VERTEX_H

//changes compared with pregel+
//1. "value" and "active" are made public members for easy programming
//2. "EdgeT" is added as an additional template argument
//3. edges are put in another stream, not present in vertex object

#include "global.h"
#include "Msg.h"
#include "serialization.h"
#include "ioser.h"
#include "Combiner.h"
using namespace std;

//Default Hash Function =====================
template <class KeyT>
class DefaultHash {
public:
    inline int operator()(KeyT key)
    {
        if (key >= 0)
            return key % _num_workers;
        else
            return (-key) % _num_workers;
    }
};
//==========================================

template <class KeyT, class ValueT, class EdgeT, class MessageT, class CombinerT = Combiner<MessageT>, class HashT = DefaultHash<KeyT> >
class Vertex {
public:
    KeyT id;
    ValueT value;
    bool active;
    int degree;
    //--- just for basic mode
    size_t pos; //position in v-array

    typedef KeyT KeyType;
    typedef ValueT ValueType;
    typedef EdgeT EdgeType;
    typedef MessageT MessageType;
    typedef HashT HashType;
    typedef CombinerT CombinerType;
    typedef vector<MessageT> MessageContainer;
    typedef vector<EdgeT> EdgeContainer;
    typedef Vertex<KeyT, ValueT, EdgeT, MessageT, CombinerT, HashT> VertexT;

    Vertex() : active(true){}

    inline bool operator<(const VertexT& rhs) const
    {
        return id < rhs.id;
    }
    inline bool operator==(const VertexT& rhs) const
    {
        return id == rhs.id;
    }
    inline bool operator!=(const VertexT& rhs) const
    {
        return id != rhs.id;
    }

    inline void activate()
    {
        active = true;
    }
    inline void vote_to_halt()
    {
        active = false;
    }

    //--- just for basic mode
    VertexID recoded_id()
    {
    	return _num_workers * pos + _my_rank;
    }

    virtual void compute(MessageContainer& msgs, EdgeContainer& edges) = 0;

    void send_message(const KeyT& id, const MessageT& msg)
    {
    	hasMsg(); //cannot end yet even every vertex halts
    	//prepare content (in fact, it's IDMsg)
    	mem_bytes << id;
    	mem_bytes << msg;
    	//add to msg-buf
    	HashT hash;
    	append(hash(id)); //in_mem_stream is consumed
    }

    //---

    //serialize into disk stream
	friend ofbinstream& operator<<(ofbinstream& m, const VertexT& v)
	{
		m << v.id << v.value << v.active << v.degree;
		return m;
	}

	//serialize out from disk stream
	friend ifbinstream& operator>>(ifbinstream& m, VertexT& v)
	{
		m >> v.id >> v.value >> v.active >> v.degree;
		return m;
	}

    //serialize into in-mem stream
	friend obinstream& operator<<(obinstream& m, const VertexT& v)
	{
		m << v.id << v.value << v.active << v.degree;
		return m;
	}

    //serialize out from in-mem stream
	friend ibinstream& operator>>(ibinstream& m, VertexT& v)
	{
		m >> v.id >> v.value >> v.active >> v.degree;
		return m;
	}
};

#endif
