#include "Vertex.h"
#include "Comper.h"
#include "Runner.h"
#include "Combiner.h"
#include <float.h>

//#define WEIGHTED_GRAPH

int src = 0;

struct SPEdge
{
    double len;
    VertexID nb;
};

obinstream & operator<<(obinstream & m, const SPEdge & e)
{
    m << e.len;
    m << e.nb;
    return m;
}
ibinstream & operator>>(ibinstream & m, SPEdge & e)
{
    m >> e.len;
    m >> e.nb;
    return m;
}
ofbinstream & operator<<(ofbinstream & m, const SPEdge & e)
{
    m << e.len;
    m << e.nb;
    return m;
}
ifbinstream & operator>>(ifbinstream & m, SPEdge & e)
{
    m >> e.len;
    m >> e.nb;
    return m;
}

class SPCombiner:public Combiner<double>
{
	public:
		virtual void combine(double& old, const double& new_msg)
		{
            if (old > new_msg) old = new_msg;
		}
};

//class SPVertex:public Vertex<VertexID, double, VertexID, double> //no combiner
class SPVertex:public Vertex<VertexID, double, SPEdge, double, SPCombiner> //with combiner
{
	public:
        void broadcast(vector<SPEdge>& edges)
        {
            for (int i = 0; i < edges.size(); ++ i) send_message(edges[i].nb, value+edges[i].len);
        }
		virtual void compute(vector<double>& msgs, vector<SPEdge>& edges)
		{
			if(step_num()==1)
			{
				if (id == src)
                {
                    value = 0;
                    broadcast(edges);
                }
                else
                {
                    value = DBL_MAX;
                }
			}
			else
			{
                double min = DBL_MAX;
                for (int i = 0; i < msgs.size(); i++)
                {
                    if (msgs[i] < min) min = msgs[i];
                }
                if (min < value)
                {
                    value = min;
                    broadcast(edges);
                }
			}
			vote_to_halt();
		}
};



class SPComper:public Comper<SPVertex>
{
	char buf[100];

	public:
#ifndef WEIGHTED_GRAPH
        //unweighted graph
		virtual VertexID parseVertex(char* line, obinstream& file_stream)
		{
			char * pch = strtok(line, "\t");
			VertexID id = atoll(pch);
			file_stream << id; //write <I>
			file_stream << DBL_MAX; //write <V>, init DBL_MAX
            if (id == src)
			    file_stream << true; //write <active>
            else 
                file_stream << false;
			pch=strtok(NULL, " ");
			int num=atoi(pch);
			file_stream << num; //write numNbs
			for(int i=0; i<num; i++)
			{
				pch=strtok(NULL, " ");
				VertexID nb = atoll(pch);
				file_stream << 1.0 << nb; //write <E>
			}
			return id;
		}
#else
        //weighted graph
		virtual VertexID parseVertex(char* line, obinstream& file_stream)
		{
			cout<<"here"<<endl;
			char * pch = strtok(line, "\t");
			VertexID id = atoll(pch);
			file_stream << id; //write <I>
			file_stream << DBL_MAX; //write <V>, init DBL_MAX
            if (id == src)
			    file_stream << true; //write <active>
            else 
                file_stream << false;
			pch=strtok(NULL, " ");
			int num=atoi(pch);
			file_stream << num; //write numNbs
			for(int i=0; i<num; i++)
			{
				pch=strtok(NULL, " ");
				VertexID nb = atoll(pch);
				pch=strtok(NULL, " ");
				double len = atof(pch);

				file_stream << len << nb; //write <E>
			}
			return id;
		}
#endif
        /*
		virtual void to_line(SPVertex& v, ofstream& fout)
		{
			fout<<v.id<<'\t'<<v.value<<endl; //report: vid \t distance
		}
        */

		virtual void to_line(SPVertex& v, BufferedWriter& fout)
		{
			if (v.value != DBL_MAX) sprintf(buf, "%lld\t%lf\n", v.id, v.value);
            else sprintf(buf, "%lld\tunreachable\n", v.id);
			fout.write(buf);
		}
};

int main(int argc, char* argv[])
{
	Runner<SPVertex, SPComper> runner;
    src = atoi(argv[3]);
	string hdfs_inpath = argv[1];
	string hdfs_outpath = argv[2];
	string local_root = "iopregel_localspace";
	bool dump_with_edges = false;
	runner.runHH(hdfs_inpath, hdfs_outpath, local_root, dump_with_edges, argc, argv); //HDFS Load, HDFS Dump
    return 0;
}