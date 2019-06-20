package edu.hanyang.submit;

import java.io.IOException;
import java.util.ArrayList;

import edu.hanyang.indexer.DocumentCursor;
import edu.hanyang.indexer.PositionCursor;
import edu.hanyang.indexer.IntermediateList;
import edu.hanyang.indexer.IntermediatePositionalList;
import edu.hanyang.indexer.QueryPlanTree;
import edu.hanyang.indexer.QueryPlanTree.NODE_TYPE;
import edu.hanyang.indexer.QueryPlanTree.QueryPlanNode;
import edu.hanyang.indexer.QueryProcess;
import edu.hanyang.indexer.StatAPI;

public class TinySEQueryProcess implements QueryProcess {

	@Override
	public void op_and_w_pos(DocumentCursor op1, DocumentCursor op2, int shift, IntermediatePositionalList out)
			throws IOException {
		
		int docID1, docID2;
		PositionCursor q1, q2;
		int pos1, pos2;

		while(op1.is_eol() == false && op2.is_eol() == false) {
			
			docID1 = op1.get_docid();
			docID2 = op2.get_docid();
			
			if(docID1 < docID2) {
				op1.go_next();				
			}else if(docID1 > docID2) {
				op2.go_next();
			}
			else {									// 같은 경우-> 두 단어를 포함한 문서내에서 단어에 대한 position -> sequence/shift
				q1 = op1.get_position_cursor();
				q2 = op2.get_position_cursor();
				
				while(q1.is_eol() == false && q2.is_eol() == false) {
					
					pos1 = q1.get_pos();
					pos2 = q2.get_pos();
					
					if(pos1 + shift < pos2) {
						q1.go_next();						
					}else if(pos1 + shift > pos2) {
						q2.go_next();
					}
					else {
						out.put_docid_and_pos(docID1, pos1);
						q1.go_next();
						q2.go_next();
					}					
				}
			}
		}
	}
	
	@Override
	public void op_and_wo_pos(DocumentCursor op1, DocumentCursor op2, IntermediateList out) throws IOException {
		
		int docID1, docID2;
		
		while(op1.is_eol() == false && op2.is_eol() == false) {
			
			docID1 = op1.get_docid();
			docID2 = op2.get_docid();
			
			if(docID1 < docID2) {
				op1.go_next();				
			}else if(docID1 > docID2) {
				op2.go_next();
			}
			else {
				out.put_docid(docID1);
				op1.go_next();
				op2.go_next();				
			}
		}
	}

	@Override
	public QueryPlanTree parse_query(String query, StatAPI stat) throws Exception {
		
		QueryPlanTree tree = new QueryPlanTree();
		QueryPlanNode node, operator;
		Boolean in_phase = false;
		ArrayList<QueryPlanNode> nodelist = new ArrayList<QueryPlanNode>();	// OP_AND,OP_SHIFTED_AND 뭐가 만들어지지 모르기에 left에 뭘 담을지 모르기에 
		int shift = 0;  													// 처음 때 OP_SHIFTED_AND 생성 하니까 자기 자신과 shift=0
		
		String[] words = query.split(" ");
		
		for(String word : words) {
			
			node = tree.new QueryPlanNode();
			node.type = NODE_TYPE.OPRAND;
			node.termid =  Integer.parseInt((word.replace("\"", "")));				
			
			if(word.charAt(0) == '\"') {						
				in_phase = true;
				shift = 0;
			}
			
			if(in_phase == false) {								// 새로운 단어에 대해 앞의 node의 경우는 op_remove_pos 뿐 1) "을 끝으로 하는 단어 or 2) 그냥
				operator = tree.new QueryPlanNode();
				operator.type = NODE_TYPE.OP_REMOVE_POS;
				operator.left = node;
				node = operator;
				
				if(nodelist.isEmpty()) {
//					System.out.println("Start "+query);
					nodelist.add(node);							// op_remove_pos 들어감
				}else {
					operator = tree.new QueryPlanNode();
					operator.type = NODE_TYPE.OP_AND;
					operator.left = nodelist.get(nodelist.size()-1);
					operator.right = node;
					nodelist.remove(nodelist.size()-1);
					nodelist.add(operator);						// op_and 들어감
				}	
			}
			else {									
				if(nodelist.isEmpty()) {
//					System.out.println("Start "+query);
				}
				// in_phase == true : 처음이 아니면 무조건 shift_and의 right
				if(shift == 0) {
					nodelist.add(node);  							// oprand 들어감 - nodelist empty여부로 안됨 - op_and나 op_remove_pos때문	
				}
				// "의 첫시작이 아닌 새로운 in_phase 단어에 대해  1) 그냥 or 2) "을 끝으로 하는 단어 -> nodelist 앞 node 1) shift_and 2) first "node-oprand
				else {
					operator = tree.new QueryPlanNode();
					operator.type = NODE_TYPE.OP_SHIFTED_AND;
					operator.left = nodelist.get(nodelist.size()-1);
					operator.right = node;
					operator.shift = shift;
					node = operator;
					nodelist.remove(nodelist.size()-1);
					
					if(word.charAt(word.length()-1) == '\"') {
						operator = tree.new QueryPlanNode();
						operator.type = NODE_TYPE.OP_REMOVE_POS;
						operator.left = node;
						node = operator;
						
						in_phase = false;
					}
					
					nodelist.add(node);								// shifted_and 또는 op_remove_pos 들어감
					shift ++;
				}					
			}
			// 쿼리 마지막은 단어 혹은 "을 끝으로 하는 단어 -> 1) op_remove_pos 단어 하나 2) op_remove_pos ""묶음 3) op_and 단어 묶음->right 채워짐
		}
		QueryPlanNode root = tree.new QueryPlanNode();
		root = nodelist.get(0);
		nodelist.remove(nodelist.remove(0));
		while(!nodelist.isEmpty()) {
			operator = tree.new QueryPlanNode();
			operator.type = NODE_TYPE.OP_AND;
			operator.left = root;
			operator.right = nodelist.get(0);
			nodelist.remove(0);
			root = operator;
		}
		tree.root = root;
//		System.out.println(nodelist.size()+"_"+tree.root.type);
//		System.out.println("done");
		return tree;
	}

}
