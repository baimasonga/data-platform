"""A bounded arithmetic grammar compiled to SQL, never evaluated as Python."""
import ast
import math
import re

from app.workflows import quote

DERIVED=('ratio','expression')


def parse_expression(source):
    if not source.strip():raise ValueError('Enter a calculation using measure IDs.')
    if len(source)>1000 or not re.fullmatch(r'[a-z0-9_+*/().\sEe-]+',source):
        raise ValueError('Calculations support measure IDs, numbers, +, -, *, / and parentheses only.')
    try:root=ast.parse(source.strip(),mode='eval').body
    except (SyntaxError,ValueError,RecursionError) as exc:raise ValueError('Invalid calculation. Use measure IDs and arithmetic operators.') from exc
    refs=set();count=0
    def visit(node,depth=0):
        nonlocal count
        count+=1
        if count>64 or depth>12:raise ValueError('Calculation is too complex: at most 64 nodes and 12 nesting levels.')
        if isinstance(node,ast.Name) and re.fullmatch(r'[a-z][a-z0-9_]{0,39}',node.id):refs.add(node.id)
        elif isinstance(node,ast.Constant) and type(node.value) in (int,float):
            if abs(node.value)>1e15 or not math.isfinite(node.value):raise ValueError('Calculation constants must be finite and no greater than 1e15 in magnitude.')
        elif isinstance(node,ast.BinOp) and type(node.op) in (ast.Add,ast.Sub,ast.Mult,ast.Div):
            visit(node.left,depth+1);visit(node.right,depth+1)
        elif isinstance(node,ast.UnaryOp) and type(node.op) in (ast.UAdd,ast.USub):visit(node.operand,depth+1)
        else:raise ValueError('Calculations support measure IDs, numbers, +, -, *, / and parentheses only.')
    visit(root)
    if not refs:raise ValueError('A calculation must reference at least one measure.')
    return root,refs


def calculation_plan(measures):
    """Validate references/cycles and return a dependency-first plan for all fields."""
    trees={};dependencies={}
    for key,measure in measures.items():
        kind=measure['aggregate']
        if kind=='expression':trees[key],dependencies[key]=parse_expression(measure.get('expression',''))
        elif kind=='ratio':dependencies[key]={measure['numerator'],measure['denominator']}
        else:dependencies[key]=set()
        if not dependencies[key]<=measures.keys():raise ValueError('Unknown measure ID in calculation: '+', '.join(sorted(dependencies[key]-measures.keys())))
    order=[];active=set();done=set()
    def visit(key):
        if key in active:raise ValueError('Calculated measures cannot contain circular references.')
        if key in done:return
        active.add(key)
        for dep in sorted(dependencies[key]):visit(dep)
        active.remove(key);done.add(key);order.append(key)
    for key in measures:visit(key)
    return trees,dependencies,order


def expression_sql(node):
    if isinstance(node,ast.Name):return quote('_m_'+node.id)
    if isinstance(node,ast.Constant):return repr(node.value)
    if isinstance(node,ast.UnaryOp):return '('+('+' if isinstance(node.op,ast.UAdd) else '-')+expression_sql(node.operand)+')'
    left,right=expression_sql(node.left),expression_sql(node.right)
    if isinstance(node.op,ast.Div):return '(CAST('+left+' AS DOUBLE)/NULLIF(CAST('+right+' AS DOUBLE),0))'
    return '('+left+{ast.Add:'+',ast.Sub:'-',ast.Mult:'*'}[type(node.op)]+right+')'
