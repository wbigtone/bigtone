package com.haizhi.utils

import java.security.MessageDigest

/**
  * Created by gikieng on 17/8/22.
  */
object KeyUtils {
	def md5(s: String): String = {
		val bytes = MessageDigest.getInstance("MD5").digest(s.getBytes())
		val hex_digest = Array[Char]('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')
		val rs = new StringBuffer()
		for (i <- 0.to(15)) {
			rs.append(hex_digest(bytes(i) >>> 4 & 0xf))
			rs.append(hex_digest(bytes(i) & 0xf))
		}
		rs.toString
	}

	def companyKeyBuild(company_name: String): String =
		md5(company_name)

	def personKeyBuild(person_name: String, company_name: String): String =
		md5("%s%s".format(person_name, company_name))

	def legalEdgeKeyBuild(person_name: String, company_name: String): String =
		md5("%s%slegal".format(person_name, company_name))

	def investEdgeKeyBuild(from_name: String, to_name: String): String =
		md5("%s%sinvest".format(from_name, to_name))

	def officeEdgeKeyBuild(officer_name: String, company_name: String, position_name: String): String =
		md5("%s%s%s".format(officer_name, company_name, position_name))

	def branchEdgeKeyBuild(branch_company: String, company_name: String): String =
		md5("%sbelong%s".format(branch_company, company_name))

	def edgeKeyBuild(from: String, to: String, edge_type: String): String =
		md5("%s%s%s".format(from, edge_type, to))
}
