import React, { useEffect, useState } from "react";
import "./Feed.css";
import Post from "./Post";
import TweetBox from "./TweetBox";
import Search from "@mui/icons-material/Search";
import axios from "axios";
import { Box } from "@mui/material";
import Users from "./Users";




function Feed({activeState,setActiveState}) {
  const [posts, setPosts] = useState([]);
  const [users,setUsers] = useState([])
  const [searchBy, setSearchBy] = useState('')
  function search(search, tweets){
    console.log('Search Criteria is')
    console.log(search)
    if(tweets==false){
      if(search.substring(0,1) == '#')
      {
        setActiveState('SEARCHBYHASH')
        setSearchBy(search)
      }
      else if(search.substring(0,1)=='@'){
        setActiveState('SEARCHBYUSERID')
        setSearchBy(search)
      }
      else{
        setActiveState('SEARCHBYTEXT')
        setSearchBy(search)
      }
    }
    else{
      if(search.substring(0,1)=='@'){
        setActiveState('SEARCHTWEETSBYUSERID')
        setSearchBy(search)
      }
    }
  }

  const Input = () => {
    const handleKeyDown = (event) => {
      if (event.key === 'Enter') {
        search(document.getElementById('searchBy').value,false)
      }
    }
  
    return <input id='searchBy' placeholder="Search Twitter"  type="text" onKeyDown={handleKeyDown}/>
  }
  useEffect(() => {
    console.log('Calling API')
    var api = 'http://localhost:8000/recenttweets'
    if(activeState=='TWEETS'){
        api = 'http://localhost:8000/trendingtweets'
    }
    else if(activeState == 'USERS'){
        api = 'http://localhost:8000/trendingusers'
    }
    else if(activeState=='SEARCHBYTEXT' || activeState=='SEARCHBYUSERID'){
        api = 'http://localhost:8000/filterby?search='.concat(searchBy)
    }
    else if (activeState=='SEARCHBYHASH'){
      api = 'http://localhost:8000/filterby?search='.concat(searchBy.substring(1)).concat('&&ishashtag=true')
    }
    else if(activeState=='SEARCHTWEETSBYUSERID'){
      api='http://localhost:8000/gettweetsbyuserid?user_id='.concat(searchBy)
    }

    axios.get(api)
    .then(response=>{
        console.log(api)
        console.log('Received response from the recenttweets endpoint')
        console.log(response)
        if(activeState!='USERS' && activeState!='SEARCHBYUSERID'){
          setPosts(response.data)
        }
        else{
          console.log('Setting the users state with the response data')
          setUsers(response.data)
        }
    })
    .catch(error=>{
        console.log('Received error from recenttweets endpoint')
        console.log(error)
    })
    // setPosts([{displayName:'Elon Musk', username:'Musk123',verified:true, text:'Twitter is now X',avatar:'C:\Users\harsh\Desktop\twitter\twitter\src\Default_profile_pic.jpg' }])
  }, [activeState,searchBy]);

  return (
    <div className="feed">
      <div className="feed__header">
        <h2>Home</h2>
      </div>
      <div className="widgets__input">
        <Search className="widgets__searchIcon" />
        <Input />
      </div>
      {
        (activeState === 'USERS' || activeState=='SEARCHBYUSERID') && <Users users={users} searchByUser={search}/>
      }
      {(activeState != 'USERS' && activeState!='SEARCHBYUSERID') && posts.map((post) => (
        <Box sx={{borderBottom:1, borderTop:1,borderColor:'#e6ecf0'}}>
            <Post
            displayName={post.user_name}
            username={post.user_id}
            verified={post.verified}
            text={post.text}
            avatar={'C:\Users\harsh\Desktop\twitter\twitter\src\Default_profile_pic.jpg'}
            retweet_count={post.retweet_count}
            like_count={post.likes_count}
            created_at={post.created_at}
            />
        </Box>
      ))}
    </div>
  );
}

export default Feed;