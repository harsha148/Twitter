import React from "react";
import "./SidebarOption.css";

function SidebarOption({ text, Icon, active ,ondivclick}) {
  return (
    <div className={`sidebarOption  ${active && "sidebarOption--active"}`} onClick={()=>{
        console.log('div clicked');
        ondivclick();
        }}>
      <Icon />
      <h2>{text}</h2>
    </div>
  );
}

export default SidebarOption;