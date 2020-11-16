/*
package com.sys.es;


@Controller
public class SearchController {

	@Autowired
	private IndexService service;
	
	@RequestMapping("/search.do")
	public String search(String keyword,int num,int count,Model m){
		PageBean<HtmlBean> page =service.search(keyword, num, count);
		m.addAttribute("page", page);
		return "/index.jsp";
	}
}
*/
