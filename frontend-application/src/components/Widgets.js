import SearchIcon from '@mui/icons-material/Search';
import React from "react";
import {
  TwitterTimelineEmbed,
  TwitterShareButton,
  TwitterTweetEmbed,
} from "react-twitter-embed";
import "./Widgets.css";
import TrendingHashtags from './TrendingHashtags';
import { Box } from '@mui/material';
import { DateTimePicker, LocalizationProvider } from '@mui/x-date-pickers';
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs';

function Widgets() {
  return (
    <div className="widgets">
        <div>
        <Box sx= {{borderBottom:1,mt:2,ml:2}}>
        <Box sx={{borderBottom:1, pb:2}}>
            <h2>Search Filter</h2>
        </Box>
            <LocalizationProvider dateAdapter={AdapterDayjs}>
                <DateTimePicker label='Start Date' sx={{mt:2,mb:2,ml:5}}/>
                <DateTimePicker label='End Date' sx={{mt:2,mb:2,ml:5}}/>
            </LocalizationProvider>
        </Box>  
        </div>
        
      <div className="widgets__widgetContainer">
        
        <Box sx={{borderBottom:1, pb:2}}>
            <h2>What's happening</h2>
        </Box>
        <TrendingHashtags/>
      </div>
    </div>
  );
}

export default Widgets;