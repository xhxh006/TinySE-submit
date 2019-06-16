package edu.hanyang.submit;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import edu.hanyang.indexer.BPlusTree;

class Node extends ArrayList{     
	int current; // position of this node
	int type;             
	int count;   // # of keys
	int parent_position;
	ArrayList<Integer> key = new ArrayList<Integer>();        // array of keys
	ArrayList<Integer> value = new ArrayList<Integer>();
	ArrayList<Integer> pointer = new ArrayList<Integer>();    // array of children' position
		
	public Node() {
		this.current = 0;
		this.type = 0;  				// 1 : non-leaf, 2: leaf
		this.count = 0; 
		this.parent_position = -1;		// whether root is
	}
}

public class TinySEBPlusTree implements BPlusTree{

	int blocksize;
	int nblocks;
	byte[] buf;
	ByteBuffer buffer;
	int maxkeys;
	Node root;
	RandomAccessFile raf;	

	@Override
	public void close() {
//		try {
//			System.out.println("B+tree--------------------");
//			for(int i=0;i< raf.length();i=i+blocksize) {
//				Node temp = readData(i);
//				System.out.println("type:"+temp.type+"root:"+temp.parent_position+"current : "+temp.current+"count : "+temp.count+"key:"+temp.key);
//			}
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		try {
			raf.close();
			buffer.clear();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	// no return type
	// 2 case : split - leaf node 
	//                - non-leaf node
	public void insert(int arg0, int arg1) {
		int key = arg0;
		int value = arg1;
		
		Node leaf = search_leaf(key);
		
		if(leaf.count == 0) {					// 빈파일 1)
			leaf.key.add(key);
			leaf.value.add(value);
		}else {
			if(key > leaf.key.get(leaf.count-1)) {
				leaf.key.add(key);
				leaf.value.add(value);
			}else {
				for(int i=0; i<leaf.count; i++) {
				if(key < leaf.key.get(i)) {
					leaf.key.add(i,key);
					leaf.value.add(i,value);
					break;
					}
				}
			}
		}
		leaf.count = leaf.key.size();

		if(leaf.count > maxkeys){
			
			Node parent = split(leaf);
			writeData(parent,parent.current);
			root = parent;
		}else {
			writeData(leaf,leaf.current);
		}
	}
		
	public Node split(Node child) {         					// make parent -> left & right -> writeData
		
		int half_rule = Math.round(((float)maxkeys+1)/2);		// N,fanout = maxkeys+1 [N/2] = half_rule
		int key_add;
		int pointer_add;
		Node left;
		Node right = new Node();
		
		if(child.type == 1) {									// non-left node split pointer [N/2] -> original & key[N/2]-> parent
			
			Node non_leaf = child;
			try {
				right.current = (int) raf.length();
			} catch (IOException e) {
				e.printStackTrace();
			}
			key_add = non_leaf.key.get(half_rule-1);
			pointer_add = right.current;
						
			right.type = non_leaf.type;
			right.key = new ArrayList<>(non_leaf.key.subList(half_rule,non_leaf.count));
			right.pointer = new ArrayList<>(non_leaf.pointer.subList(half_rule,non_leaf.pointer.size()));
			right.count = right.key.size();
																							// -> original 
			non_leaf.key = new ArrayList<>(non_leaf.key.subList(0,half_rule-1));
			non_leaf.pointer = new ArrayList<>(non_leaf.pointer.subList(0,half_rule));
			non_leaf.count = non_leaf.key.size();
			
			for(int i =0; i < right.pointer.size();i++) {
				Node children = readData(right.pointer.get(i));
				children.parent_position = right.current;
				writeData(children,children.current);
			}
			left = non_leaf;
			
		}else {														// left node split : key [N/2] -> original
			Node leaf = child;

			try {
				right.current = (int) raf.length();
			} catch (IOException e) {
				e.printStackTrace();
			}
					
			right.type = leaf.type;
			right.key = new ArrayList<>(leaf.key.subList(half_rule,leaf.count));
			right.value = new ArrayList<>(leaf.value.subList(half_rule,leaf.count));
			right.count = right.key.size();
																			       // -> original 
			leaf.key = new ArrayList<>(leaf.key.subList(0,half_rule));
			leaf.value = new ArrayList<>(leaf.value.subList(0,half_rule));
			leaf.count = leaf.key.size();

			key_add = right.key.get(0);
			pointer_add = right.current;
			left = leaf;

		}
		
		if (left.parent_position < 0) {  					// 2)인 경우
			Node parent = new Node();
			parent.type = 1;
			try {
				parent.current = (int)raf.length()+blocksize;
			} catch (IOException e) {
				e.printStackTrace();
			}
			parent.key.add(key_add);
			parent.pointer.add(left.current);
			parent.pointer.add(right.current);
			left.parent_position = parent.current;
			right.parent_position = parent.current;
			parent.count = parent.key.size();
			
			writeData(left,left.current);
			writeData(right,right.current);
						
			return parent;
			
		}else {												// 3) 부모가 있음
			Node parent = readData(left.parent_position);	// 추가 하고 count 비교 위와 같이 
			if(key_add > parent.key.get(parent.count-1)) {
				parent.key.add(key_add);
				parent.pointer.add(pointer_add);
			}else {
				for(int i=0; i< parent.count; i++) {
					if(key_add < parent.key.get(i)) {
						parent.key.add(i,right.key.get(0));
						parent.pointer.add(i+1,right.current);    // ** i+1 번째에 right.current = right.key보다 큰 값들
						break;
						}
				}
			}

			parent.count = parent.key.size();
			left.parent_position = parent.current;
			right.parent_position = parent.current;
			writeData(left,left.current);
			writeData(right,right.current);
					
			if(parent.count > maxkeys) {								// non-leaf is full -> split 재귀
				parent = split(parent);
			}													// -> insert
			
			return parent;
			
		}
	}

	@Override
	// metapath, savepath, blocksize, nblocks
	// node_size = blocksize, 메인메모리에 가능한 담아서 nblocks
	public void open(String metapath, String savepath, int blocksize, int nblocks) {
		this.blocksize = blocksize;  // 52
		this.nblocks = nblocks;
		this.buf = new byte[blocksize];
		this.buffer = ByteBuffer.wrap(buf);
		this.maxkeys = (blocksize - 16)/8;   // 4
		
		try {
			raf = new RandomAccessFile(savepath, "rw");
			root = new Node();
			
			if (raf.length() != 0) {						// file 3번째 처음 쓰여 있지만 -> root가 아님
				root = readData(0);
				while(root.parent_position > 0) {		
					root = readData(root.parent_position);					
				}
			}else {											// 빈파일 -> insert돼도 2번 case니까
				root.type = 2;
			}
		}catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public int search(int key) {
		Node leaf = search_leaf(key);
		int value = -1;
		
		for(int i=0;i < leaf.count; i++) {
	         if(key == leaf.key.get(i)) {
	        	 value = leaf.value.get(i);
	        	 }
		}
		return value;
	}
	
	// all_values >= 0 -> not exist = return -1
	public Node search_leaf(int arg0) {
		int key = arg0;
		int pos = -1;
		Node cur = new Node();
		cur = root;							        // root에서 부터 찾아감

		while(cur.type != 2) {			            // 3) root에서 leaf찾아감 type으로
			if(key >= cur.key.get(cur.count-1)) {
				pos = cur.pointer.get(cur.count);
			}else {
				for(int i =0; i < cur.count;i++) {
					if (key < cur.key.get(i)) {    	   	// 값 작으면 그 앞의 pointer로
						pos = cur.pointer.get(i);
						break;
						}
					}
				}
			cur = readData(pos);			// 이동
		}        		
		return cur;							// 2) 빈파일이여도 type이 2인 경우
	}
	
	public Node readData(int position) {
		byte[] buf = new byte[this.blocksize];
		ByteBuffer buffer = ByteBuffer.wrap(buf);
		Node temp = new Node();
		try {
			raf.seek(position);
			raf.read(buf);
			} catch(IOException e){
				e.printStackTrace();
				}
		temp.current = buffer.getInt();
		temp.type = buffer.getInt();
		temp.count = buffer.getInt();
		temp.parent_position = buffer.getInt();
		
		if(temp.count != 0) {
			if(temp.type == 2) {
				for(int i = 0; i < temp.count; i++) {
					temp.key.add(buffer.getInt());
					temp.value.add(buffer.getInt());
					}
				}
			else {
				for(int i = 0; i < temp.count; i++) {
					temp.key.add(buffer.getInt());
					}
				for(int j = 0; j < temp.count+1; j++) {
					temp.pointer.add(buffer.getInt());
					}
				}
			}
		return temp;
		}
	
	public void writeData(Node node, int position) {
		byte[] buf = new byte[this.blocksize];
		ByteBuffer buffer = ByteBuffer.wrap(buf);
	
		buffer.putInt(node.current);
		buffer.putInt(node.type);
		buffer.putInt(node.count);
		buffer.putInt(node.parent_position);
	   
		if(node.count != 0) {
			if(node.type == 2) {
				for(int i = 0; i < node.count; i++) {
					buffer.putInt(node.key.get(i).intValue());
					buffer.putInt(node.value.get(i).intValue());
					}
				}
			else {
				for(int i = 0; i < node.count; i++) {
					buffer.putInt(node.key.get(i).intValue());
					}
				for(int j = 0; j < node.count+1; j++) {
					buffer.putInt(node.pointer.get(j).intValue());
					}
				}
			}
		try {
			raf.seek(position);
			raf.write(buf);
			} catch(IOException e) {
				e.printStackTrace();
				}
		}
	   
}
