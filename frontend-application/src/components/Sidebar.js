import React,{useState} from "react";
import "./Sidebar.css";
import SidebarOption from "./SidebarOption";
import XIcon from '@mui/icons-material/X';
import HomeIcon from '@mui/icons-material/Home';
import SearchIcon from '@mui/icons-material/Search';
import NotificationsNoneIcon from '@mui/icons-material/NotificationsNone';
import MailOutlineIcon from '@mui/icons-material/MailOutline';
import BookmarkBorderIcon from '@mui/icons-material/BookmarkBorder';
import ListAltIcon from '@mui/icons-material/ListAlt';
import PermIdentityIcon from '@mui/icons-material/PermIdentity';
import MoreHorizIcon from '@mui/icons-material/MoreHoriz';
import Button from '@mui/material/Button';
import { TrendingUp } from "@mui/icons-material";




function Sidebar({setActive}) {
    const [homeActive, setHomeActive] = useState(true)
    const [trendingTweetsActive, setTrendingTweetsActive] = useState(false)
    const [trendingUsersActive, setTrendingUsersActive] = useState(false)
    function sethome(){
        setActive('HOME')
        setHomeActive(true)
        setTrendingTweetsActive(false)
        setTrendingUsersActive(false)
    }
    function settweetsactive(){
        setActive('TWEETS')
        setTrendingTweetsActive(true)
        setHomeActive(false)
        setTrendingUsersActive(false)
    }
    function setusersactive(){
        setActive('USERS')
        setTrendingTweetsActive(false)
        setHomeActive(false)
        setTrendingUsersActive(true)
    }
    function donothing(){

    }
  return (
    <div className="sidebar">
      <XIcon className="sidebar__twitterIcon" sx={{color:"black"}}/>
      <SidebarOption Icon={HomeIcon} text="Home" active={homeActive} ondivclick={sethome}/>
      <SidebarOption Icon={TrendingUp} text="Trending Tweets" active={trendingTweetsActive} ondivclick={settweetsactive}/>
      <SidebarOption Icon={TrendingUp} text="Trending Users" active={trendingUsersActive} ondivclick={setusersactive}/>
      <SidebarOption Icon={NotificationsNoneIcon} text="Notifications" ondivclick={donothing}/>
      <SidebarOption Icon={MailOutlineIcon} text="Messages" ondivclick={donothing}/>
      <SidebarOption Icon={BookmarkBorderIcon} text="Bookmarks" ondivclick={donothing}/>
      <SidebarOption Icon={PermIdentityIcon} text="Profile" ondivclick={donothing}/>
      <SidebarOption Icon={MoreHorizIcon} text="More" ondivclick={donothing}/>

      <Button variant="outlined" className="sidebar__tweet" fullWidth>
        Tweet
      </Button>
    </div>
  );
}

export default Sidebar;