package com.util


import java.lang.reflect.{Method, Modifier}

import scala.util.control.Breaks

/**
 * 实现对象属性拷贝
 */
object MyBeanUtils {

  /**
   * 将 srcObj 中属性的值拷贝到 destObj 对应的属性上.
   */
  def copyProperties(srcObj: AnyRef, destObj: AnyRef): Unit = {
    if (srcObj == null && destObj == null) {
      return
    }

    // 获取srcobj中的所有属性
    val srcFields = srcObj.getClass().getDeclaredFields

    // 处理每个属性的拷贝
    for (srcField <- srcFields) {
      Breaks.breakable {
        // get  set
        // scala 会自动为类中的属性提供set get方法
        // get : fieldname()
        // set : fieldname_$eq(参数类型)

        // getMethodName
        val getMethodName: String = srcField.getName
        // setMethodName
        val setMethodName = srcField.getName + "_$eq"

        // 从srcObj中获取get方法
        val getMethod = srcObj.getClass().getDeclaredMethod(getMethodName)

        // 从destObj中获取set方法
        // String name;
        // getName()
        // setName(String name ){ this.name = name }

        val setMethod: Method =
          try {
            destObj.getClass().getDeclaredMethod(setMethodName, srcField.getType)
          } catch {
            case ex: Exception => Breaks.break()
          }

        // 忽略val属性
        val destField = destObj.getClass.getDeclaredField(srcField.getName)
        if (destField.getModifiers.equals(Modifier.FINAL)) {
          Breaks.break()
        }

        // 调用get方法获取到srcObj属性，再嗲用set方法将获取到的属性值 赋值给destObj的属性
        setMethod.invoke(destObj, getMethod.invoke(srcObj))

      }
    }
  }
}
