##' Function trace back to the primary item starting from an edge-item.
##' It uses the commodity tree to go back level by level untill the first one.
##'
##' @param edge a CPC (or a vector of CPC codes) 
##' @param tree Commodity tree, data.table
##' @param standParams computation parameters
##'
##' @export
##'

getPrimary = function(edge, tree, standParams){
    
    ## Since we'll be editing the tree, make a copy so we don't overwrite
    ## things.
    origTree = copy(tree[, c(standParams$parentVar, standParams$childVar,
                             "processingLevel"),
                         with = FALSE])
    
    
    
    output = origTree[processingLevel == 0 & get(standParams$childVar) %in% edge,]
    
    
    
    editedTree = copy(origTree)
    ## Stop if we have a very flat tree:
    if (max(tree$processingLevel) >= 1) {
        for (i in 1:(max(tree$processingLevel) - 1)) {
            ## Roll down availability
            copyTree = copy(origTree)
            copyTree=copyTree[get(standParams$childVar) %in% edge,]
            # Rename parent as child and child as 'newChild'
            setnames(copyTree, c(standParams$parentVar, standParams$childVar),
                     c(standParams$childVar, "newChild"))
            editedTree[, c( "processingLevel") := NULL]
            editedTree = merge(editedTree, copyTree, by = c(standParams$childVar),
                               suffixes = c("", ".child"), allow.cartesian = TRUE)
            
            editedTree[, newChild:= NULL]
            
            output = rbind(output,editedTree)
        }
    }
    ## Because we extract edges at each level, we could possibly still get
    ## multiple edges (i.e. A is a parent of B and C, but C is also a parent of
    ## B).  It's a weird case, but it exists and we need to handle it.  So, just
    ## average the availabilities in those cases.
    output = unique(output[,  get(standParams$parentVar)])
    
    
    return(output)
}