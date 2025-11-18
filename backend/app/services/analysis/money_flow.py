"""Money flow analysis service"""
import logging
from typing import List, Dict, Any

from app.services.fec_client import FECClient
from app.models.schemas import MoneyFlowNode, MoneyFlowEdge, MoneyFlowGraph

logger = logging.getLogger(__name__)


class MoneyFlowService:
    """Service for money flow graph analysis"""
    
    def __init__(self, fec_client: FECClient):
        self.fec_client = fec_client
    
    async def build_money_flow_graph(
        self,
        candidate_id: str,
        max_depth: int = 2,
        min_amount: float = 100.0,
        aggregate_by_employer: bool = True
    ) -> MoneyFlowGraph:
        """Build network graph of money flows"""
        nodes = []
        edges = []
        node_ids = set()
        
        # Get candidate info
        candidate = await self.fec_client.get_candidate(candidate_id)
        if candidate:
            candidate_node_id = f"candidate_{candidate_id}"
            nodes.append(MoneyFlowNode(
                id=candidate_node_id,
                name=candidate.get('name', 'Unknown'),
                type="candidate",
                amount=None
            ))
            node_ids.add(candidate_node_id)
        
        # Get committees
        committees = await self.fec_client.get_committees(candidate_id=candidate_id)
        committee_nodes = {}
        
        for committee in committees:
            committee_id = committee.get('committee_id')
            if committee_id:
                committee_node_id = f"committee_{committee_id}"
                committee_nodes[committee_id] = committee_node_id
                if committee_node_id not in node_ids:
                    nodes.append(MoneyFlowNode(
                        id=committee_node_id,
                        name=committee.get('name', 'Unknown Committee'),
                        type="committee",
                        amount=None
                    ))
                    node_ids.add(committee_node_id)
                
                # Add edge from committee to candidate
                if candidate:
                    edges.append(MoneyFlowEdge(
                        source=committee_node_id,
                        target=f"candidate_{candidate_id}",
                        amount=0.0,
                        type="committee_to_candidate"
                    ))
        
        # Get contributions
        contributions = await self.fec_client.get_contributions(
            candidate_id=candidate_id,
            min_amount=min_amount,
            limit=5000
        )
        
        if aggregate_by_employer:
            # Group contributions by employer
            employer_contributions = {}
            employer_donors = {}
            
            for contrib in contributions:
                employer = contrib.get('contributor_employer') or 'Unknown Employer'
                donor_name = contrib.get('contributor_name')
                committee_id = contrib.get('committee_id')
                amount = contrib.get('contribution_amount', 0.0)
                
                if committee_id:
                    if employer not in employer_contributions:
                        employer_contributions[employer] = {}
                        employer_donors[employer] = set()
                    if committee_id not in employer_contributions[employer]:
                        employer_contributions[employer][committee_id] = 0.0
                    employer_contributions[employer][committee_id] += amount
                    if donor_name:
                        employer_donors[employer].add(donor_name)
            
            # Add employer nodes and edges
            for employer, committee_amounts in list(employer_contributions.items())[:50]:
                employer_node_id = f"employer_{hash(employer) % 1000000}"
                if employer_node_id not in node_ids:
                    total_employer_amount = sum(committee_amounts.values())
                    donor_count = len(employer_donors.get(employer, set()))
                    display_name = f"{employer} ({donor_count} donor{'s' if donor_count != 1 else ''})"
                    nodes.append(MoneyFlowNode(
                        id=employer_node_id,
                        name=display_name[:50],
                        type="employer",
                        amount=total_employer_amount
                    ))
                    node_ids.add(employer_node_id)
                
                # Add edges from employer to committees
                for committee_id, amount in committee_amounts.items():
                    if committee_id in committee_nodes:
                        edges.append(MoneyFlowEdge(
                            source=employer_node_id,
                            target=committee_nodes[committee_id],
                            amount=float(amount),
                            type="contribution"
                        ))
        else:
            # Group contributions by donor
            donor_contributions = {}
            for contrib in contributions:
                donor_name = contrib.get('contributor_name')
                committee_id = contrib.get('committee_id')
                amount = contrib.get('contribution_amount', 0.0)
                
                if donor_name and committee_id:
                    if donor_name not in donor_contributions:
                        donor_contributions[donor_name] = {}
                    if committee_id not in donor_contributions[donor_name]:
                        donor_contributions[donor_name][committee_id] = 0.0
                    donor_contributions[donor_name][committee_id] += amount
            
            # Add donor nodes and edges
            for donor_name, committee_amounts in list(donor_contributions.items())[:100]:
                donor_node_id = f"donor_{hash(donor_name) % 1000000}"
                if donor_node_id not in node_ids:
                    total_donor_amount = sum(committee_amounts.values())
                    nodes.append(MoneyFlowNode(
                        id=donor_node_id,
                        name=donor_name[:50],
                        type="donor",
                        amount=total_donor_amount
                    ))
                    node_ids.add(donor_node_id)
                
                # Add edges from donor to committees
                for committee_id, amount in committee_amounts.items():
                    if committee_id in committee_nodes:
                        edges.append(MoneyFlowEdge(
                            source=donor_node_id,
                            target=committee_nodes[committee_id],
                            amount=float(amount),
                            type="contribution"
                        ))
        
        return MoneyFlowGraph(nodes=nodes, edges=edges)

