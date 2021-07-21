package com.bayee.petition.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import org.springframework.web.multipart.MultipartFile;

public class FileUtile {

	/**
	 * 保存文件
	 * 
	 * @param file
	 * @param request
	 * @return
	 * @throws IOException
	 */
	public String addFile(MultipartFile file, HttpServletRequest request) throws IOException {

		// 存储文件
		byte[] bytes = file.getBytes();
		// 生成唯一标识 UUID
		String uuid = UUID.randomUUID().toString();
		// 文件名
		String fileName = file.getOriginalFilename().substring(0, file.getOriginalFilename().lastIndexOf(".")) + uuid;
		// 文件存储路径
		//String url = request.getSession().getServletContext().getRealPath("/resources/feedbackPicture/");
		//String url="C:/upload-test";
		
		String url = "/mnt/petition-system";
		System.out.println(url);
		// 文件后缀名
		String suffix = file.getOriginalFilename().substring(file.getOriginalFilename().lastIndexOf("."));
		// 加上后缀的文件名
		String nameBelong = fileName + suffix;
		Path path = Paths.get(url, url+"/"+nameBelong);
		Files.write(path, bytes);

		return nameBelong;

	};
}
