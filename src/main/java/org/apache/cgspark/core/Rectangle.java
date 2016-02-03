package org.apache.cgspark.core;

import java.io.Serializable;

/**
 * Rectangle object
 * @author prashantchaudhary
 *
 */
public class Rectangle implements Serializable{
  private static final long serialVersionUID = -9141798944008110788L;
  public int l, r, t, b;
  
  public Rectangle() {
    new Rectangle(0, 0, 0, 0);
  }
  
  public Rectangle (int l, int r, int b, int t) {
    this.l = l;
    this.r = r;
    this.b = b;
    this.t = t;
  }

  @Override
  public String toString() {
    return "l=" + l + ",r=" + r + ",b=" + b + ",t=" + t;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + b;
    result = prime * result + l;
    result = prime * result + r;
    result = prime * result + t;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Rectangle other = (Rectangle) obj;
    if (b != other.b)
      return false;
    if (l != other.l)
      return false;
    if (r != other.r)
      return false;
    if (t != other.t)
      return false;
    return true;
  }
}
