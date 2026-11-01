"""A bounded calculation grammar compiled to SQL, never evaluated as Python."""
import ast
import math
import re

from app.workflows import quote

DERIVED=('ratio','expression')
FUNCTIONS=[
    {'name':'abs','signature':'abs(value)','description':'Absolute value; missing inputs stay missing.','example':'abs(profit)'},
    {'name':'round','signature':'round(value[, digits])','description':'Round to 0–8 decimal places (default 0); halfway values round away from zero.','example':'round(profit / revenue * 100, 2)'},
    {'name':'coalesce','signature':'coalesce(value, fallback[, ...])','description':'First non-null value, with 2–8 arguments.','example':'coalesce(profit / revenue * 100, 0)'},
    {'name':'if_else','signature':'if_else(condition, when_true, when_false)','description':'Compare numeric measures using ==, !=, <, <=, >, >=, and/or/not. A null condition chooses when_false.','example':'if_else(revenue > 100, profit, 0)'},
]
SYNTAX_ERROR='Use numeric measures, arithmetic, abs(), round(), coalesce() or if_else().'
COMPARE={ast.Eq:'=',ast.NotEq:'<>',ast.Lt:'<',ast.LtE:'<=',ast.Gt:'>',ast.GtE:'>='}


def parse_expression(source):
    if not source.strip():raise ValueError('Enter a calculation using measure IDs.')
    if len(source)>1000 or not re.fullmatch(r'[a-z0-9_+*/(),<>=!.\sEe-]+',source):
        raise ValueError(SYNTAX_ERROR)
    try:root=ast.parse(source.strip(),mode='eval').body
    except (SyntaxError,ValueError,RecursionError) as exc:raise ValueError('Invalid calculation. Check parentheses and use == for equality.') from exc
    refs=set();count=0
    def visit(node,depth=0,condition=False):
        nonlocal count
        count+=1
        if count>64 or depth>12:raise ValueError('Calculation is too complex: at most 64 nodes and 12 nesting levels.')
        if condition:
            if isinstance(node,ast.Compare) and len(node.ops)==1 and type(node.ops[0]) in COMPARE:
                visit(node.left,depth+1);visit(node.comparators[0],depth+1)
            elif isinstance(node,ast.BoolOp) and type(node.op) in (ast.And,ast.Or):
                for value in node.values:visit(value,depth+1,True)
            elif isinstance(node,ast.UnaryOp) and isinstance(node.op,ast.Not):visit(node.operand,depth+1,True)
            else:raise ValueError('if_else conditions require numeric comparisons combined with and, or, not.')
            return
        if isinstance(node,ast.Name) and re.fullmatch(r'[a-z][a-z0-9_]{0,39}',node.id):refs.add(node.id)
        elif isinstance(node,ast.Constant) and type(node.value) in (int,float):
            if abs(node.value)>1e15 or not math.isfinite(node.value):raise ValueError('Calculation constants must be finite and no greater than 1e15 in magnitude.')
        elif isinstance(node,ast.BinOp) and type(node.op) in (ast.Add,ast.Sub,ast.Mult,ast.Div):
            visit(node.left,depth+1);visit(node.right,depth+1)
        elif isinstance(node,ast.UnaryOp) and type(node.op) in (ast.UAdd,ast.USub):visit(node.operand,depth+1)
        elif isinstance(node,ast.Call):
            if not isinstance(node.func,ast.Name) or node.keywords:raise ValueError('Use supported functions with positional arguments only.')
            name=node.func.id;args=node.args
            if name=='abs' and len(args)==1:visit(args[0],depth+1)
            elif name=='round' and len(args) in (1,2):
                visit(args[0],depth+1)
                if len(args)==2:
                    digits=args[1]
                    if not isinstance(digits,ast.Constant) or type(digits.value) is not int or not 0<=digits.value<=8:
                        raise ValueError('round digits must be a literal integer from 0 to 8.')
                    visit(digits,depth+1)
            elif name=='coalesce' and 2<=len(args)<=8:
                for arg in args:visit(arg,depth+1)
            elif name=='if_else' and len(args)==3:
                visit(args[0],depth+1,True);visit(args[1],depth+1);visit(args[2],depth+1)
            else:raise ValueError('Unknown function or invalid argument count. Use abs(value), round(value[, digits]), coalesce(2–8 values), or if_else(condition, true_value, false_value).')
        else:raise ValueError(SYNTAX_ERROR)
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
    if isinstance(node,ast.Compare):return '('+expression_sql(node.left)+COMPARE[type(node.ops[0])]+expression_sql(node.comparators[0])+')'
    if isinstance(node,ast.BoolOp):return '('+(' AND ' if isinstance(node.op,ast.And) else ' OR ').join(expression_sql(value) for value in node.values)+')'
    if isinstance(node,ast.Call):
        args=[expression_sql(arg) for arg in node.args]
        if node.func.id=='if_else':return '(CASE WHEN '+args[0]+' THEN '+args[1]+' ELSE '+args[2]+' END)'
        return {'abs':'ABS','round':'ROUND','coalesce':'COALESCE'}[node.func.id]+'('+','.join(args)+')'
    if isinstance(node,ast.UnaryOp):
        operator='NOT ' if isinstance(node.op,ast.Not) else '+' if isinstance(node.op,ast.UAdd) else '-'
        return '('+operator+expression_sql(node.operand)+')'
    left,right=expression_sql(node.left),expression_sql(node.right)
    if isinstance(node.op,ast.Div):return '(CAST('+left+' AS DOUBLE)/NULLIF(CAST('+right+' AS DOUBLE),0))'
    return '('+left+{ast.Add:'+',ast.Sub:'-',ast.Mult:'*'}[type(node.op)]+right+')'
