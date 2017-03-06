/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package databasehomework;

/**
 *
 * @author suyas
 */
public class Dependency {
    
    private String left;
    private String right;
    
    public Dependency(String left, String right){
        this.left = left;
        this.right = right;
    }
    
    public String getLeft(){
        return this.left;
    }
    
    public String getRight(){
        return this.right;
    }
    
    public String showDep(){
        return this.left+"-->"+this.right;
    }
    
}
