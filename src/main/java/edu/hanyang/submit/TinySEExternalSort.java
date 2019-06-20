package edu.hanyang.submit;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import edu.hanyang.indexer.ExternalSort;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.lucene.util.ByteBlockPool.DirectAllocator;


public class TinySEExternalSort implements ExternalSort {
			
	public void sort(String infile, String outfile, String tmpdir, int blocksize, int nblocks) throws IOException {
		int nElement = (blocksize * nblocks)/((Integer.SIZE/Byte.SIZE)*3); 						//�� Run�� Ʃ���� ����
		ArrayList<MutableTriple<Integer,Integer,Integer>> dataArr = new ArrayList<>(nElement);
		
		File dir = new File(tmpdir);
		dir.mkdir();
		
		DataInputStream input = new DataInputStream(new BufferedInputStream(
				new FileInputStream(infile),blocksize));
		
		int step = 0;
		while(input.available() > 0) {
			for(int i =0; i < nElement; i++) {
				MutableTriple<Integer,Integer,Integer> muta = new MutableTriple<>();
				try {
					muta.setLeft(input.readInt());
					muta.setMiddle(input.readInt());
					muta.setRight(input.readInt());
					dataArr.add(muta);
				}catch(EOFException e) {
					break;
				}
			}
			Collections.sort(dataArr);
			
			dir = new File(tmpdir+File.separator+"0");
			if(!dir.exists()) {
				dir.mkdir();
			}
			
			DataOutputStream output = new DataOutputStream(new BufferedOutputStream(
					new FileOutputStream(dir+File.separator+"run1-"+step+".data"),blocksize));
						
			for(MutableTriple<Integer,Integer,Integer> temp : dataArr) {
				output.writeInt(temp.getLeft());
				output.writeInt(temp.getMiddle());
				output.writeInt(temp.getRight());
			}
			dataArr.clear();
			output.close();
			step++;

		}
		
		input.close();
		
		_externalMergeSort(tmpdir,outfile,0,nblocks,blocksize);
	// }
	
	// private void _externalMergeSort(String tmpDir, String outputFile, int step,int nblocks,int blocksize) throws IOException {

	// 	File[] fileArr = (new File(tmpDir + File.separator + String.valueOf(step))).listFiles();
		
		
	// 	if (fileArr.length <= nblocks - 1) {
			
			
	// 		List<DataInputStream> files = new ArrayList<>();
	// 		for (File f : fileArr) {
	// 			DataInputStream dos = new DataInputStream(new BufferedInputStream(
	// 					new FileInputStream (f.getAbsolutePath()),blocksize));
	// 			files.add(dos);				
	// 		}
	// 		merge(files,outputFile,blocksize);
	// 	}
	// 	else {
						
	// 		List<DataInputStream> files = new ArrayList<>();
	// 		int cnt = 0;
	// 		int run_step = 0;
	// 		for (File f : fileArr) {	
	// 			DataInputStream dos = new DataInputStream(new BufferedInputStream(
	// 					new FileInputStream (f.getAbsolutePath()),blocksize));
	// 			files.add(dos);
	// 			cnt++;
	// 			if (cnt == nblocks - 1) {
	// 				n_way_merge(files, tmpDir + File.separator + String.valueOf(step+1),run_step,blocksize);
	// 				run_step++;
	// 			}
	// 		}
	// 	_externalMergeSort(tmpDir, outputFile, step+1, nblocks, blocksize);
	// 	}
	// }
	
	// public void n_way_merge(List<DataInputStream> files, String outputFile,int run_step,int blocksize) throws IOException {
		
	// 	File dir = new File(outputFile);
	// 	if(!dir.exists()) {
	// 		dir.mkdir();
	// 	}
		
	// 	DataOutputStream output = new DataOutputStream(new BufferedOutputStream(
	// 			new FileOutputStream(outputFile),blocksize));
		
	// 	PriorityQueue<DataManager> queue = new PriorityQueue<>(files.size(), new Comparator<DataManager>() {
	// 		public int compare(DataManager o1, DataManager o2) {
	// 			return o1.tuple.compareTo(o2.tuple);
	// 			}
	// 		});
		
	// 	for(DataInputStream f : files) {
	// 		try{
	// 			DataManager dm = new DataManager(f.readInt(),f.readInt(),f.readInt(),files.indexOf(f));
	// 			queue.add(dm);
				
	// 		}catch(EOFException e) {
	// 			continue;
	// 		}
	// 	}
		
	// 	while (queue.size() != 0) {
	// 		try {
	// 			DataManager dm = queue.poll();
	// 			MutableTriple<Integer, Integer, Integer> tmp = dm.getTuple();
										
	// 			output.writeInt(tmp.getLeft());
	// 			output.writeInt(tmp.getMiddle());
	// 			output.writeInt(tmp.getRight());
							
	// 			dm.setTuple(files.get(dm.index).readInt(),files.get(dm.index).readInt(),files.get(dm.index).readInt());

	// 			queue.add(dm);
				
	// 		}catch(EOFException e) {
	// 			continue;
	// 		} 
	// 	}
	// 	output.close();
	// }
	
	}
	
	private void _externalMergeSort(String tmpDir, String outputFile, int step,int nblocks,int blocksize) throws IOException {

		File[] fileArr = (new File(tmpDir + File.separator + String.valueOf(step))).listFiles();
		
		
		if (fileArr.length <= nblocks - 1) {
			
			List<DataInputStream> files = new ArrayList<>();
			for (File f : fileArr) {
				DataInputStream dos = new DataInputStream(new BufferedInputStream(
						new FileInputStream (f.getAbsolutePath()),blocksize));
				files.add(dos);				
			}
			merge(files,outputFile,blocksize);
		}
		else {
						
			List<DataInputStream> files = new ArrayList<>();
			int cnt = 0;
			int run_step = 0;
			for (File f : fileArr) {	
				DataInputStream dos = new DataInputStream(new BufferedInputStream(
						new FileInputStream (f.getAbsolutePath()),blocksize));
				files.add(dos);
				cnt++;
				if (cnt == nblocks - 1){
					n_way_merge(files, tmpDir + File.separator + String.valueOf(step+1),run_step,blocksize);
					run_step++;
					cnt = 0;
					files.clear();
					System.out.println(run_step);
				}				
			}
			if(files.size() > 0) {
				n_way_merge(files, tmpDir + File.separator + String.valueOf(step+1),run_step,blocksize);
				files.clear();
			}
			System.out.println("check"+run_step);	
			_externalMergeSort(tmpDir, outputFile, step+1, nblocks, blocksize);
		}
	}
	
	public void n_way_merge(List<DataInputStream> files, String outputFile,int run_step,int blocksize) throws IOException {
		
		File dir = new File(outputFile);
		if(!dir.exists()) {
			dir.mkdir();
		}
	
		DataOutputStream output = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(outputFile+File.separator+String.valueOf(run_step)+".data"),blocksize));
		
		PriorityQueue<DataManager> queue = new PriorityQueue<>(files.size(), new Comparator<DataManager>() {
			public int compare(DataManager o1, DataManager o2) {
				return o1.tuple.compareTo(o2.tuple);
				}
			});
		
		for(DataInputStream f : files) {
			try{
				DataManager dm = new DataManager(f.readInt(),f.readInt(),f.readInt(),files.indexOf(f));
				queue.add(dm);
				
			}catch(EOFException e) {
				continue;
			}
		}
		
		while (queue.size() != 0) {
			try {
				DataManager dm = queue.poll();
				MutableTriple<Integer, Integer, Integer> tmp = dm.getTuple();
										
				output.writeInt(tmp.getLeft());
				output.writeInt(tmp.getMiddle());
				output.writeInt(tmp.getRight());
							
				dm.setTuple(files.get(dm.index).readInt(),files.get(dm.index).readInt(),files.get(dm.index).readInt());

				queue.add(dm);
				
			}catch(EOFException e) {
				continue;
			} 
		}

		output.close();
	}
	
	public void merge(List<DataInputStream> files, String outputFile,int blocksize) throws IOException {
		
		DataOutputStream output = new DataOutputStream(new BufferedOutputStream(
				new FileOutputStream(outputFile),blocksize));
		
		PriorityQueue<DataManager> queue = new PriorityQueue<>(files.size(), new Comparator<DataManager>() {
			public int compare(DataManager o1, DataManager o2) {
				return o1.tuple.compareTo(o2.tuple);
				}
			});
		
		for(DataInputStream f : files) {
			try{
				DataManager dm = new DataManager(f.readInt(),f.readInt(),f.readInt(),files.indexOf(f));
				queue.add(dm);
				
			}catch(EOFException e) {
				continue;
			}
		}
		
		while (queue.size() != 0) {
			try {
				DataManager dm = queue.poll();
				MutableTriple<Integer, Integer, Integer> tmp = dm.getTuple();
										
				output.writeInt(tmp.getLeft());
				output.writeInt(tmp.getMiddle());
				output.writeInt(tmp.getRight());
							
				dm.setTuple(files.get(dm.index).readInt(),files.get(dm.index).readInt(),files.get(dm.index).readInt());

				queue.add(dm);
				
			}catch(EOFException e) {
				continue;
			} 
		}
		output.close();
	}
}

class DataManager {
	
	MutableTriple<Integer, Integer, Integer> tuple;
	int index;
	
	   public DataManager(int left, int middle, int right, int index) {
		   tuple = new MutableTriple<Integer, Integer, Integer>(left,middle,right); 
		   this.index = index;
	   }
	   public void setTuple(int left, int middle, int right) {
		   tuple.setLeft(left);
		   tuple.setMiddle(middle);
		   tuple.setRight(right);
	   }
	   public MutableTriple<Integer, Integer, Integer> getTuple() {
	      return this.tuple;
	   }
}	
